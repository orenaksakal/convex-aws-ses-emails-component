import { v } from "convex/values";
import {
  internalAction,
  mutation,
  type MutationCtx,
  query,
  internalQuery,
  type ActionCtx,
} from "./_generated/server.js";
import { Workpool } from "@convex-dev/workpool";
import { RateLimiter } from "@convex-dev/rate-limiter";
import { api, components, internal } from "./_generated/api.js";
import { internalMutation } from "./_generated/server.js";
import { type Id, type Doc } from "./_generated/dataModel.js";
import {
  ACCEPTED_EVENT_TYPES,
  type RuntimeConfig,
  vEmailEvent,
  vOptions,
  vStatus,
  vTemplate,
} from "./shared.js";
import type { FunctionHandle } from "convex/server";
import type { EmailEvent, RunMutationCtx, RunQueryCtx } from "./shared.js";
import { isDeepEqual } from "remeda";
import schema from "./schema.js";
import { omit } from "convex-helpers";
import { parse } from "convex-helpers/validators";
import { assertExhaustive, attemptToParse } from "./utils.js";
import {
  SESv2Client,
  SendEmailCommand,
  type SendEmailCommandInput,
} from "@aws-sdk/client-sesv2";

// Debug logging - enable with SES_DEBUG=true environment variable
const isDebug = () => process.env.SES_DEBUG === "true";
const debug = (...args: unknown[]) => {
  if (isDebug()) console.log("[SES]", ...args);
};

// Move some of these to options? TODO
const SEGMENT_MS = 125;
const BASE_BATCH_DELAY = 1000;
const BATCH_SIZE = 100;
const EMAIL_POOL_SIZE = 4;
const CALLBACK_POOL_SIZE = 4;
const SES_ONE_CALL_EVERY_MS = 100; // SES has high rate limits
const FINALIZED_EMAIL_RETENTION_MS = 1000 * 60 * 60 * 24 * 7; // 7 days
const FINALIZED_EPOCH = Number.MAX_SAFE_INTEGER;
const ABANDONED_EMAIL_RETENTION_MS = 1000 * 60 * 60 * 24 * 30; // 30 days

const PERMANENT_ERROR_CODES = new Set([
  400, 401 /* 402 not included - unclear spec */, 403, 404, 405, 406, 407, 408,
  /* 409 not included - conflict may work on retry */
  410, 411 /* 412 not included - precondition may have changed? */, 413, 414,
  415, 416 /* 417, not included - expectation may be met later? */, 418, 421,
  422 /*423, 424, 425, may change over time */, 426, 427,
  428 /* 429, explicitly asked to retry */, 431 /* 451, laws change? */,
]);

// We break the emails into segments to avoid contention on new emails being inserted.
function getSegment(now: number) {
  return Math.floor(now / SEGMENT_MS);
}

// Four threads is more than enough, especially given the low rate limiting.
const emailPool = new Workpool(components.emailWorkpool, {
  maxParallelism: EMAIL_POOL_SIZE,
});

// We need to run callbacks in a separate pool so we don't tie up too many threads.
const callbackPool = new Workpool(components.callbackWorkpool, {
  maxParallelism: CALLBACK_POOL_SIZE,
});

// We rate limit our calls to the SES API.
const sesApiRateLimiter = new RateLimiter(components.rateLimiter, {
  sesApi: {
    kind: "fixed window",
    period: SES_ONE_CALL_EVERY_MS,
    rate: 1,
  },
});

// Periodic background job to clean up old emails that have already
// been delivered, bounced, what have you.
export const cleanupOldEmails = mutation({
  args: { olderThan: v.optional(v.number()) },
  returns: v.null(),
  handler: async (ctx, args) => {
    const BATCH_SIZE = 100;
    const olderThan = args.olderThan ?? FINALIZED_EMAIL_RETENTION_MS;
    const oldAndDone = await ctx.db
      .query("emails")
      .withIndex("by_finalizedAt", (q) =>
        q.lt("finalizedAt", Date.now() - olderThan),
      )
      .take(BATCH_SIZE);
    for (const email of oldAndDone) {
      await cleanupEmail(ctx, email);
    }
    if (oldAndDone.length > 0) {
      debug(`Cleaned up ${oldAndDone.length} emails`);
    }
    if (oldAndDone.length === BATCH_SIZE) {
      await ctx.scheduler.runAfter(0, api.lib.cleanupOldEmails, {
        olderThan,
      });
    }
  },
});

// Enqueue an email to be send.  A background job will grab batches
// of emails and enqueue them to be sent by the workpool.
export const sendEmail = mutation({
  args: {
    options: vOptions,
    from: v.string(),
    to: v.array(v.string()),
    cc: v.optional(v.array(v.string())),
    bcc: v.optional(v.array(v.string())),
    subject: v.optional(v.string()),
    html: v.optional(v.string()),
    text: v.optional(v.string()),
    template: v.optional(vTemplate),
    replyTo: v.optional(v.array(v.string())),
    headers: v.optional(
      v.array(
        v.object({
          name: v.string(),
          value: v.string(),
        }),
      ),
    ),
  },
  returns: v.id("emails"),
  handler: async (ctx, args) => {
    // In SES sandbox mode, you can only send to verified email addresses.
    // This is enforced by the SES API, not client-side.
    // We require either html/text or a template to be provided.
    const hasContent = args.html !== undefined || args.text !== undefined;
    const hasTemplate = args.template?.name !== undefined;

    if (!hasContent && !hasTemplate) {
      throw new Error("Either html/text or template must be provided");
    }
    if (hasContent && hasTemplate) {
      throw new Error("Cannot provide both html/text and template");
    }
    if (!hasTemplate && args.subject === undefined) {
      throw new Error("Subject is required when not using a template");
    }

    // Store the text/html into separate records to keep things fast and memory low when we work with email batches.
    let htmlContentId: Id<"content"> | undefined;
    if (args.html !== undefined) {
      const contentId = await ctx.db.insert("content", {
        content: new TextEncoder().encode(args.html).buffer,
        mimeType: "text/html",
      });
      htmlContentId = contentId;
    }

    let textContentId: Id<"content"> | undefined;
    if (args.text !== undefined) {
      const contentId = await ctx.db.insert("content", {
        content: new TextEncoder().encode(args.text).buffer,
        mimeType: "text/plain",
      });
      textContentId = contentId;
    }

    // This is the "send requested" segment.
    const segment = getSegment(Date.now());

    // Okay, we're ready to insert the email into the database, waiting for a background job to enqueue it.
    const emailId = await ctx.db.insert("emails", {
      from: args.from,
      to: args.to,
      cc: args.cc,
      bcc: args.bcc,
      subject: args.subject,
      html: htmlContentId,
      text: textContentId,
      template: args.template,
      headers: args.headers,
      segment,
      status: "waiting",
      bounced: false,
      complained: false,
      failed: false,
      deliveryDelayed: false,
      opened: false,
      clicked: false,
      replyTo: args.replyTo ?? [],
      finalizedAt: FINALIZED_EPOCH,
    });

    // Ensure there is a worker running to grab batches of emails.
    await scheduleBatchRun(ctx, args.options);
    return emailId;
  },
});

export const createManualEmail = mutation({
  args: {
    from: v.string(),
    to: v.union(v.array(v.string()), v.string()),
    subject: v.string(),
    replyTo: v.optional(v.array(v.string())),
    headers: v.optional(
      v.array(
        v.object({
          name: v.string(),
          value: v.string(),
        }),
      ),
    ),
  },
  returns: v.id("emails"),
  handler: async (ctx, args) => {
    const emailId = await ctx.db.insert("emails", {
      from: args.from,
      to: Array.isArray(args.to) ? args.to : [args.to],
      subject: args.subject,
      headers: args.headers,
      segment: Infinity,
      status: "queued",
      bounced: false,
      complained: false,
      failed: false,
      deliveryDelayed: false,
      opened: false,
      clicked: false,
      replyTo: args.replyTo ?? [],
      finalizedAt: FINALIZED_EPOCH,
    });
    return emailId;
  },
});

export const updateManualEmail = mutation({
  args: {
    emailId: v.id("emails"),
    status: vStatus,
    sesMessageId: v.optional(v.string()),
    errorMessage: v.optional(v.string()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const finalizedAt =
      args.status === "failed" || args.status === "cancelled"
        ? Date.now()
        : undefined;
    await ctx.db.patch(args.emailId, {
      status: args.status,
      sesMessageId: args.sesMessageId,
      errorMessage: args.errorMessage,
      ...(finalizedAt ? { finalizedAt } : {}),
    });
  },
});

// Cancel an email that has not been sent yet. The worker will ignore it
// within whatever batch it is in.
export const cancelEmail = mutation({
  args: {
    emailId: v.id("emails"),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const email = await ctx.db.get(args.emailId);
    if (!email) {
      throw new Error("Email not found");
    }
    if (email.status !== "waiting" && email.status !== "queued") {
      throw new Error("Email has already been sent");
    }
    await ctx.db.patch(args.emailId, {
      status: "cancelled",
      finalizedAt: Date.now(),
    });
  },
});

// Get the status of an email.
export const getStatus = query({
  args: {
    emailId: v.id("emails"),
  },
  returns: v.union(
    v.object({
      status: vStatus,
      errorMessage: v.union(v.string(), v.null()),
      bounced: v.boolean(),
      complained: v.boolean(),
      failed: v.boolean(),
      deliveryDelayed: v.boolean(),
      opened: v.boolean(),
      clicked: v.boolean(),
    }),
    v.null(),
  ),
  handler: async (ctx, args) => {
    const email = await ctx.db.get(args.emailId);
    if (!email) {
      return null;
    }
    return {
      status: email.status,
      errorMessage: email.errorMessage ?? null,
      bounced: email.bounced ?? false,
      complained: email.complained,
      failed: email.failed ?? false,
      deliveryDelayed: email.deliveryDelayed ?? false,
      opened: email.opened,
      clicked: email.clicked ?? false,
    };
  },
});

// Get the entire email.
export const get = query({
  args: {
    emailId: v.id("emails"),
  },
  returns: v.union(
    v.object({
      ...omit(schema.tables.emails.validator.fields, ["html", "text"]),
      createdAt: v.number(),
      html: v.optional(v.string()),
      text: v.optional(v.string()),
      to: v.array(v.string()),
    }),
    v.null(),
  ),
  handler: async (ctx, args) => {
    const email = await ctx.db.get(args.emailId);
    if (!email) {
      return null;
    }
    const html = email.html
      ? new TextDecoder().decode((await ctx.db.get(email.html))?.content)
      : undefined;
    const text = email.text
      ? new TextDecoder().decode((await ctx.db.get(email.text))?.content)
      : undefined;
    return {
      ...omit(email, ["html", "text", "_id", "_creationTime"]),
      createdAt: email._creationTime,
      html,
      text,
      to: Array.isArray(email.to) ? email.to : [email.to],
    };
  },
});

// Ensure there is a worker running to grab batches of emails.
async function scheduleBatchRun(ctx: MutationCtx, options: RuntimeConfig) {
  // Update the last options if they've changed.
  const lastOptions = await ctx.db.query("lastOptions").unique();
  if (!lastOptions) {
    await ctx.db.insert("lastOptions", {
      options,
    });
  } else if (!isDeepEqual(lastOptions.options, options)) {
    await ctx.db.replace(lastOptions._id, {
      options,
    });
  }

  // Check if there is already a worker running.
  const existing = await ctx.db.query("nextBatchRun").unique();

  // Is there already a worker running?
  if (existing) {
    return;
  }

  // No worker running? Schedule one.
  const runId = await ctx.scheduler.runAfter(
    BASE_BATCH_DELAY,
    internal.lib.makeBatch,
    { reloop: false, segment: getSegment(Date.now() + BASE_BATCH_DELAY) },
  );

  // Insert the new worker to reserve exactly one running.
  await ctx.db.insert("nextBatchRun", {
    runId,
  });
}

// A background job that grabs batches of emails and enqueues them to be sent by the workpool.
export const makeBatch = internalMutation({
  args: { reloop: v.boolean(), segment: v.number() },
  returns: v.null(),
  handler: async (ctx, args) => {
    // Get the options for the worker.
    const lastOptions = await ctx.db.query("lastOptions").unique();
    if (!lastOptions) {
      throw new Error("No last options found -- invariant");
    }
    const options = lastOptions.options;

    // Grab the batch of emails to send.
    const emails = await ctx.db
      .query("emails")
      .withIndex("by_status_segment", (q) =>
        // We scan earlier than two segments ago to avoid contention between new email insertions and batch creation.
        q.eq("status", "waiting").lte("segment", args.segment - 2),
      )
      .take(BATCH_SIZE);

    // If we have no emails, or we have a short batch on a reloop,
    // let's delay working for now.
    if (emails.length === 0 || (args.reloop && emails.length < BATCH_SIZE)) {
      return reschedule(ctx, emails.length > 0);
    }

    debug(`Making a batch of ${emails.length} emails`);

    // Mark the emails as queued.
    for (const email of emails) {
      await ctx.db.patch(email._id, {
        status: "queued",
      });
    }

    // Okay, let's calculate rate limiting as best we can globally in this distributed system.
    const delay = await getDelay(ctx);

    // Give the batch to the workpool! It will call the SES API
    // in a durable background action.
    await emailPool.enqueueAction(
      ctx,
      internal.lib.callSesAPIWithBatch,
      {
        region: options.region,
        accessKeyId: options.accessKeyId,
        secretAccessKey: options.secretAccessKey,
        configurationSetName: options.configurationSetName,
        emails: emails.map((e) => e._id),
      },
      {
        retry: {
          maxAttempts: options.retryAttempts,
          initialBackoffMs: options.initialBackoffMs,
          base: 2,
        },
        runAfter: delay,
        context: { emailIds: emails.map((e) => e._id) },
        onComplete: internal.lib.onEmailComplete,
      },
    );

    // Let's go around again until there are no more batches to make in this particular segment range.
    await ctx.scheduler.runAfter(0, internal.lib.makeBatch, {
      reloop: true,
      segment: args.segment,
    });
  },
});

// If there are no more emails to send in this segment range, we need to check to see if there are any
// emails in newer segments and so we should sleep for a bit before trying to make batches again.
// If the table is empty, we need to stop the worker and idle the system until a new email is inserted.
async function reschedule(ctx: MutationCtx, emailsLeft: boolean) {
  emailsLeft =
    emailsLeft ||
    (await ctx.db
      .query("emails")
      .withIndex("by_status_segment", (q) => q.eq("status", "waiting"))
      .first()) !== null;

  if (!emailsLeft) {
    // No next email yet?
    const batchRun = await ctx.db.query("nextBatchRun").unique();
    if (!batchRun) {
      throw new Error("No batch run found -- invariant");
    }
    await ctx.db.delete(batchRun._id);
  } else {
    const segment = getSegment(Date.now() + BASE_BATCH_DELAY);
    await ctx.scheduler.runAfter(BASE_BATCH_DELAY, internal.lib.makeBatch, {
      reloop: false,
      segment,
    });
  }
}

// Helper to fetch content. We'll use batch apis here to avoid lots of action->query calls.
async function getAllContent(
  ctx: ActionCtx,
  contentIds: Id<"content">[],
): Promise<Map<Id<"content">, string>> {
  const docs = await ctx.runQuery(internal.lib.getAllContentByIds, {
    contentIds,
  });
  return new Map(docs.map((doc) => [doc.id, doc.content]));
}

const vBatchReturns = v.union(
  v.null(),
  v.object({
    emailIds: v.array(v.id("emails")),
    sesMessageIds: v.array(v.string()),
  }),
);

// Call the SES API with a batch of emails.
// SES sends each email individually via the SendEmail API.
export const callSesAPIWithBatch = internalAction({
  args: {
    region: v.string(),
    accessKeyId: v.string(),
    secretAccessKey: v.string(),
    configurationSetName: v.optional(v.string()),
    emails: v.array(v.id("emails")),
  },
  returns: vBatchReturns,
  handler: async (ctx, args) => {
    // Create SES client
    const client = new SESv2Client({
      region: args.region,
      credentials: {
        accessKeyId: args.accessKeyId,
        secretAccessKey: args.secretAccessKey,
      },
    });

    // Construct the payload for the SES API from all the database values.
    const batchPayload = await createSesBatchPayload(ctx, args.emails);

    if (batchPayload === null) {
      // No emails to send.
      debug("No emails to send in batch. All were cancelled or failed.");
      return null;
    }

    const [emailIds, emailPayloads] = batchPayload;

    const successfulIds: Id<"emails">[] = [];
    const messageIds: string[] = [];
    const failedEmails: { emailId: Id<"emails">; error: string }[] = [];

    // Send each email individually since SES v2 doesn't have batch send
    for (let i = 0; i < emailPayloads.length; i++) {
      const payload = emailPayloads[i];
      const emailId = emailIds[i];

      try {
        const command = new SendEmailCommand({
          ...payload,
          ConfigurationSetName: args.configurationSetName,
        });

        const response = await client.send(command);

        if (response.MessageId) {
          successfulIds.push(emailId);
          messageIds.push(response.MessageId);
        } else {
          failedEmails.push({
            emailId,
            error: "No MessageId returned from SES",
          });
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);

        // Check if it's a permanent error
        if (
          error &&
          typeof error === "object" &&
          "$metadata" in error &&
          typeof error.$metadata === "object" &&
          error.$metadata !== null &&
          "httpStatusCode" in error.$metadata &&
          typeof error.$metadata.httpStatusCode === "number" &&
          PERMANENT_ERROR_CODES.has(error.$metadata.httpStatusCode)
        ) {
          failedEmails.push({ emailId, error: errorMessage });
        } else {
          // For transient errors, throw to trigger retry
          throw new Error(`SES API error: ${errorMessage}`);
        }
      }
    }

    // Mark any permanently failed emails
    if (failedEmails.length > 0) {
      await ctx.runMutation(internal.lib.markEmailsFailed, {
        emailIds: failedEmails.map((f) => f.emailId),
        errorMessage: `SES API errors: ${failedEmails.map((f) => f.error).join("; ")}`,
      });
    }

    if (successfulIds.length === 0) {
      return null;
    }

    return {
      emailIds: successfulIds,
      sesMessageIds: messageIds,
    };
  },
});

export const markEmailsFailed = internalMutation({
  args: {
    emailIds: v.array(v.id("emails")),
    errorMessage: v.string(),
  },
  returns: v.null(),
  handler: markEmailsFailedHandler,
});

async function markEmailsFailedHandler(
  ctx: MutationCtx,
  args: {
    emailIds: Id<"emails">[];
    errorMessage: string;
  },
) {
  await Promise.all(
    args.emailIds.map(async (emailId) => {
      const email = await ctx.db.get(emailId);
      if (!email || email.status !== "queued") {
        return;
      }
      await ctx.db.patch(emailId, {
        status: "failed",
        errorMessage: args.errorMessage,
        finalizedAt: Date.now(),
      });
    }),
  );
}

export const onEmailComplete = emailPool.defineOnComplete({
  context: v.object({
    emailIds: v.array(v.id("emails")),
  }),
  handler: async (ctx, args) => {
    if (args.result.kind === "success") {
      const result = parse(vBatchReturns, args.result.returnValue);
      if (result === null) {
        return;
      }
      const { emailIds, sesMessageIds } = result;
      await Promise.all(
        emailIds.map((emailId, i) =>
          ctx.db.patch(emailId, {
            status: "sent",
            sesMessageId: sesMessageIds[i],
          }),
        ),
      );
    } else if (args.result.kind === "failed") {
      await markEmailsFailedHandler(ctx, {
        emailIds: args.context.emailIds,
        errorMessage: args.result.error,
      });
    } else if (args.result.kind === "canceled") {
      await Promise.all(
        args.context.emailIds.map(async (emailId) => {
          const email = await ctx.db.get(emailId);
          if (!email || email.status !== "queued") {
            return;
          }
          await ctx.db.patch(emailId, {
            status: "cancelled",
            errorMessage: "SES API batch job was cancelled",
            finalizedAt: Date.now(),
          });
        }),
      );
    }
  },
});

// Helper to create the payload for the SES API.
async function createSesBatchPayload(
  ctx: ActionCtx,
  emailIds: Id<"emails">[],
): Promise<[Id<"emails">[], SendEmailCommandInput[]] | null> {
  // Fetch emails from database.
  const allEmails = await ctx.runQuery(internal.lib.getEmailsByIds, {
    emailIds,
  });
  // Filter out cancelled emails.
  const emails = allEmails.filter((e) => e.status === "queued");
  if (emails.length === 0) {
    return null;
  }
  // Fetch body content from database.
  const contentMap = await getAllContent(
    ctx,
    emails
      .flatMap((e) => [e.html, e.text])
      .filter((id): id is Id<"content"> => id !== undefined),
  );

  // Build payload for SES API.
  const payloads: SendEmailCommandInput[] = emails.map((email: Doc<"emails">) => {
    const to = Array.isArray(email.to) ? email.to : [email.to];

    // Build content - either template or simple content
    let content: SendEmailCommandInput["Content"];
    if (email.template) {
      content = {
        Template: {
          TemplateName: email.template.name,
          TemplateData: email.template.data
            ? JSON.stringify(email.template.data)
            : undefined,
        },
      };
    } else {
      const html = email.html ? contentMap.get(email.html) : undefined;
      const text = email.text ? contentMap.get(email.text) : undefined;

      content = {
        Simple: {
          Subject: {
            Data: email.subject ?? "",
            Charset: "UTF-8",
          },
          Body: {
            Html: html
              ? {
                  Data: html,
                  Charset: "UTF-8",
                }
              : undefined,
            Text: text
              ? {
                  Data: text,
                  Charset: "UTF-8",
                }
              : undefined,
          },
        },
      };
    }

    const payload: SendEmailCommandInput = {
      FromEmailAddress: email.from,
      Destination: {
        ToAddresses: to,
        CcAddresses: email.cc,
        BccAddresses: email.bcc,
      },
      ReplyToAddresses: email.replyTo && email.replyTo.length > 0 ? email.replyTo : undefined,
      Content: content,
      EmailTags: email.headers && email.headers.length > 0
        ? email.headers.map((h: { name: string; value: string }) => ({
            Name: h.name,
            Value: h.value,
          }))
        : undefined,
    };

    return payload;
  });

  return [emails.map((e) => e._id), payloads];
}

const FIXED_WINDOW_DELAY = 100;
async function getDelay(ctx: RunMutationCtx & RunQueryCtx): Promise<number> {
  const limit = await sesApiRateLimiter.limit(ctx, "sesApi", {
    reserve: true,
  });
  const jitter = Math.random() * FIXED_WINDOW_DELAY;
  return limit.retryAfter ? limit.retryAfter + jitter : 0;
}

// Helper to fetch content by id. We'll use batch apis here to avoid lots of action->query calls.
export const getAllContentByIds = internalQuery({
  args: { contentIds: v.array(v.id("content")) },
  returns: v.array(v.object({ id: v.id("content"), content: v.string() })),
  handler: async (ctx, args) => {
    const contentMap = [];
    const promises = [];
    for (const contentId of args.contentIds) {
      promises.push(ctx.db.get(contentId));
    }
    const docs = await Promise.all(promises);
    for (const doc of docs) {
      if (!doc) throw new Error("Content not found -- invariant");
      contentMap.push({
        id: doc._id,
        content: new TextDecoder().decode(doc.content),
      });
    }
    return contentMap;
  },
});

// Helper to fetch emails by id. We'll use batch apis here to avoid lots of action->query calls.
export const getEmailsByIds = internalQuery({
  args: { emailIds: v.array(v.id("emails")) },
  handler: async (ctx, args) => {
    const emails = await Promise.all(args.emailIds.map((id) => ctx.db.get(id)));

    // Some emails might be missing b/c they were cancelled long ago and already
    // cleaned up because the retention period has passed.
    return emails.filter((e): e is Doc<"emails"> => e !== null);
  },
});

// Helper to fetch an email by sesMessageId. This is used by the event handler.
// SES gives us *their* id back, not ours. We'll use the index to find it.
export const getEmailBySesMessageId = internalQuery({
  args: { sesMessageId: v.string() },
  handler: async (ctx, args) => {
    const email = await ctx.db
      .query("emails")
      .withIndex("by_sesMessageId", (q) => q.eq("sesMessageId", args.sesMessageId))
      .unique();
    if (!email) throw new Error("Email not found for sesMessageId");
    return email;
  },
});

// Compute the updated email record for a given SES event without writing it.
// Only returns an update if there's a state change to reduce write contention.
function computeEmailUpdateFromEvent(
  email: Doc<"emails">,
  event: EmailEvent,
): Doc<"emails"> | null {
  // Define precedence for statuses; only allow upgrades
  const statusRank: Record<Doc<"emails">["status"], number> = {
    waiting: 0,
    queued: 1,
    sent: 2,
    delivery_delayed: 3,
    delivered: 4,
    bounced: 5,
    failed: 5,
    cancelled: 100, // treat cancelled as terminal
  };

  const currentRank = statusRank[email.status];
  const canUpgradeTo = (next: Doc<"emails">["status"]) => {
    if (email.status === "cancelled") return false;
    return statusRank[next] > currentRank;
  };

  // NOOP -- we do this automatically when we send the email.
  if (event.eventType === "Send") return null;

  if (event.eventType === "Click") {
    // Only mutate if this is the first click
    if (email.clicked) return null;
    return {
      ...email,
      clicked: true,
    };
  }

  if (event.eventType === "Reject" || event.eventType === "Rendering Failure") {
    // Only mutate if this is the first failure OR status changes
    const statusWillChange = canUpgradeTo("failed");
    if (!statusWillChange && email.failed) {
      return null; // No state change
    }
    const updated: Doc<"emails"> = {
      ...email,
      failed: true,
    };
    if (statusWillChange) {
      updated.status = "failed";
      updated.finalizedAt = Date.now();
      if (event.eventType === "Reject") {
        updated.errorMessage = event.reject.reason;
      } else {
        updated.errorMessage = event.failure.errorMessage;
      }
    }
    return updated;
  }

  if (event.eventType === "Delivery") {
    if (!canUpgradeTo("delivered")) return null;
    return {
      ...email,
      status: "delivered",
      finalizedAt: Date.now(),
    };
  }

  if (event.eventType === "Bounce") {
    // Only mutate if this is the first bounce OR status changes
    const statusWillChange = canUpgradeTo("bounced");
    if (!statusWillChange && email.bounced) {
      return null; // No state change
    }
    const bounceMessage = event.bounce.bouncedRecipients
      .map((r) => `${r.emailAddress}: ${r.diagnosticCode ?? "unknown"}`)
      .join("; ");
    const updated: Doc<"emails"> = {
      ...email,
      errorMessage: `${event.bounce.bounceType}/${event.bounce.bounceSubType}: ${bounceMessage}`,
      bounced: true,
    };
    if (statusWillChange) {
      updated.status = "bounced";
      updated.finalizedAt = Date.now();
    }
    return updated;
  }

  if (event.eventType === "DeliveryDelay") {
    // Only mutate if this is the first delay OR status changes
    const statusWillChange = canUpgradeTo("delivery_delayed");
    if (!statusWillChange && email.deliveryDelayed) {
      return null; // No state change
    }
    const updated: Doc<"emails"> = {
      ...email,
      deliveryDelayed: true,
    };
    if (statusWillChange) {
      updated.status = "delivery_delayed";
    }
    return updated;
  }

  if (event.eventType === "Complaint") {
    // Only mutate if this is the first complaint
    if (email.complained) return null;
    return {
      ...email,
      complained: true,
      finalizedAt:
        email.finalizedAt === FINALIZED_EPOCH ? Date.now() : email.finalizedAt,
    };
  }

  if (event.eventType === "Open") {
    // Only mutate if this is the first open
    if (email.opened) return null;
    return {
      ...email,
      opened: true,
    };
  }

  assertExhaustive(event);
  return null;
}

// Handle an SES event from SNS notification. Mostly we just update the email status.
export const handleEmailEvent = mutation({
  args: {
    event: v.any(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    // Event can be anything, so we need to parse it.
    // This will also strip out anything that shouldn't be there.
    const result = attemptToParse(vEmailEvent, args.event);
    if (result.kind === "error") {
      console.warn(
        `[SES] Invalid SES event received. You might want to exclude this event from your SNS topic settings. ${result.error}.`,
      );
      return;
    }

    const event = result.data;

    const email = await ctx.db
      .query("emails")
      .withIndex("by_sesMessageId", (q) => q.eq("sesMessageId", event.mail.messageId))
      .unique();

    if (!email) {
      debug(
        `Email not found for sesMessageId: ${event.mail.messageId}, ignoring...`,
      );
      return;
    }

    // Record delivery-related events for auditing/analytics (now including opened/clicked)
    if (ACCEPTED_EVENT_TYPES.includes(event.eventType as (typeof ACCEPTED_EVENT_TYPES)[number])) {
      let message: string | undefined;
      if (event.eventType === "Bounce") {
        message = event.bounce.bouncedRecipients
          .map((r) => r.diagnosticCode)
          .filter(Boolean)
          .join("; ");
      } else if (event.eventType === "Reject") {
        message = event.reject.reason;
      } else if (event.eventType === "Rendering Failure") {
        message = event.failure.errorMessage;
      }

      await ctx.db.insert("deliveryEvents", {
        emailId: email._id,
        sesMessageId: event.mail.messageId,
        eventType: event.eventType as (typeof ACCEPTED_EVENT_TYPES)[number],
        createdAt: event.mail.timestamp,
        message,
      });
    }

    // Apply the event directly to update email state if needed
    const updated = computeEmailUpdateFromEvent(email, event);
    if (updated) {
      await ctx.db.replace(email._id, updated);
    }

    // Keep callback behavior (invoked with current email state and raw event)
    await enqueueCallbackIfExists(ctx, email, event);
  },
});

async function enqueueCallbackIfExists(
  ctx: MutationCtx,
  email: Doc<"emails">,
  event: EmailEvent,
) {
  const lastOptions = await ctx.db.query("lastOptions").unique();
  // lastOptions may not exist if the user only uses sendEmailManually
  if (!lastOptions) {
    return;
  }
  if (lastOptions.options.onEmailEvent) {
    const handle = lastOptions.options.onEmailEvent.fnHandle as FunctionHandle<
      "mutation",
      {
        id: Id<"emails">;
        event: EmailEvent;
      },
      void
    >;
    await callbackPool.enqueueMutation(ctx, handle, {
      id: email._id,
      event: event,
    });
  }
}

async function cleanupEmail(ctx: MutationCtx, email: Doc<"emails">) {
  await ctx.db.delete(email._id);
  if (email.text) {
    await ctx.db.delete(email.text);
  }
  if (email.html) {
    await ctx.db.delete(email.html);
  }
  const events = await ctx.db
    .query("deliveryEvents")
    .withIndex("by_emailId_eventType", (q) => q.eq("emailId", email._id))
    .collect();
  for (const event of events) {
    await ctx.db.delete(event._id);
  }
}

// Periodic background job to clean up old emails that have been abandoned.
// Meaning, even if they're not finalized, we should just get rid of them.
export const cleanupAbandonedEmails = mutation({
  args: { olderThan: v.optional(v.number()) },
  returns: v.null(),
  handler: async (ctx, args) => {
    const olderThan = args.olderThan ?? ABANDONED_EMAIL_RETENTION_MS;
    const oldAndAbandoned = await ctx.db
      .query("emails")
      .withIndex("by_creation_time", (q) =>
        q.lt("_creationTime", Date.now() - olderThan),
      )
      .take(500);

    for (const email of oldAndAbandoned) {
      // No webhook to finalize these. We'll just delete them.
      await cleanupEmail(ctx, email);
    }
    if (oldAndAbandoned.length > 0) {
      debug(`Cleaned up ${oldAndAbandoned.length} emails`);
    }
    if (oldAndAbandoned.length === 500) {
      await ctx.scheduler.runAfter(0, api.lib.cleanupAbandonedEmails, {
        olderThan,
      });
    }
  },
});
