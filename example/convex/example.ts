import {
  internalMutation,
  internalAction,
  internalQuery,
} from "./_generated/server";
import { components, internal } from "./_generated/api";
import { Ses, vOnEmailEventArgs } from "convex-aws-ses";
import { v } from "convex/values";
import {
  SESv2Client,
  SendEmailCommand,
} from "@aws-sdk/client-sesv2";

const sesClient = new SESv2Client({
  region: process.env.AWS_REGION ?? "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

export const ses: Ses = new Ses(components.ses, {
  onEmailEvent: internal.example.handleEmailEvent,
});

export const testBatch = internalAction({
  args: {
    from: v.string(),
    to: v.array(v.string()), // In SES, you need verified addresses
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    // In SES sandbox mode, all recipients must be verified
    // You'll need to replace these with your verified email addresses
    const addresses = args.to;

    for (let i = 0; i < Math.min(25, addresses.length); i++) {
      const address = addresses[i % addresses.length];
      const email = await ses.sendEmail(ctx, {
        from: args.from,
        to: address,
        subject: "Test Email",
        html: "This is a test email",
      });
      await ctx.runMutation(internal.example.insertExpectation, {
        email: email,
        expectation: "delivered", // SES uses sandbox mode - all recipients must be verified
      });
    }
    while (!(await ctx.runQuery(internal.example.isEmpty))) {
      console.log("Waiting for emails to be processed...");
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    console.log("All emails finalized as expected");
  },
});

export const sendOne = internalAction({
  args: {
    from: v.optional(v.string()),
    to: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    // Replace with your verified email addresses
    const from = args.from ?? "your-verified-sender@yourdomain.com";
    const to = args.to ?? "your-verified-recipient@yourdomain.com";

    const email = await ses.sendEmail(ctx, {
      from,
      to,
      subject: "Test Email",
      html: "This is a test email",
    });
    console.log("Email sent", email);
    let status = await ses.status(ctx, email);
    while (
      status &&
      (status.status === "queued" || status.status === "waiting")
    ) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      status = await ses.status(ctx, email);
    }
    console.log("Email status", status);
    return email;
  },
});

export const sendWithTemplate = internalAction({
  args: {
    from: v.optional(v.string()),
    to: v.optional(v.string()),
    templateName: v.string(),
    subject: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args) => {
    // Replace with your verified email addresses
    const from = args.from ?? "your-verified-sender@yourdomain.com";
    const to = args.to ?? "your-verified-recipient@yourdomain.com";

    const email = await ses.sendEmail(ctx, {
      from,
      to,
      subject: args.subject, // Optional: override template's default subject
      template: {
        name: args.templateName,
        data: {
          PRODUCT: "Vintage Macintosh",
          PRICE: 499,
        },
      },
    });
    console.log("Email with template sent", email);
    let status = await ses.status(ctx, email);
    while (
      status &&
      (status.status === "queued" || status.status === "waiting")
    ) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      status = await ses.status(ctx, email);
    }
    console.log("Email status", status);
    return email;
  },
});

export const insertExpectation = internalMutation({
  args: {
    email: v.string(),
    expectation: v.union(
      v.literal("delivered"),
      v.literal("bounced"),
      v.literal("complained"),
    ),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.db.insert("testEmails", {
      email: args.email,
      expectation: args.expectation,
    });
  },
});

export const isEmpty = internalQuery({
  returns: v.boolean(),
  handler: async (ctx) => {
    return (await ctx.db.query("testEmails").first()) === null;
  },
});

export const handleEmailEvent = internalMutation({
  args: vOnEmailEventArgs,
  handler: async (ctx, args) => {
    console.log("Got called back!", args.id, args.event);
    const testEmail = await ctx.db
      .query("testEmails")
      .withIndex("by_email", (q) => q.eq("email", args.id))
      .unique();
    if (!testEmail) {
      console.log("No test email found for id", args.id);
      return;
    }
    if (args.event.eventType === "Delivery") {
      if (testEmail.expectation === "bounced") {
        throw new Error("Email was delivered but expected to be bounced");
      }
      if (testEmail.expectation === "complained") {
        console.log(
          "Complained email was delivered, expecting complaint coming...",
        );
        return;
      }
      // All good. Delivered email was delivered.
      await ctx.db.delete(testEmail._id);
    }
    if (args.event.eventType === "Bounce") {
      if (testEmail.expectation !== "bounced") {
        throw new Error(
          `Email was bounced but expected to be ${testEmail.expectation}`,
        );
      }
      // All good. Bounced email was bounced.
      await ctx.db.delete(testEmail._id);
    }
    if (args.event.eventType === "Complaint") {
      if (testEmail.expectation !== "complained") {
        throw new Error(
          `Email was complained but expected to be ${testEmail.expectation}`,
        );
      }
      // All good. Complained email was complained.
      await ctx.db.delete(testEmail._id);
    }
  },
});

export const sendManualEmail = internalAction({
  args: {
    from: v.optional(v.string()),
    to: v.optional(v.union(v.string(), v.array(v.string()))),
    subject: v.optional(v.string()),
    text: v.optional(v.string()),
    html: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    // Replace with your verified email addresses
    const from = args.from ?? "your-verified-sender@yourdomain.com";
    const to = args.to ?? "your-verified-recipient@yourdomain.com";
    const subject = args.subject ?? "Test Email";
    const text = args.text ?? "This is a test email";

    const emailId = await ses.sendEmailManually(
      ctx,
      { from, to, subject },
      async (_emailId) => {
        // Send via SES SDK directly
        const command = new SendEmailCommand({
          FromEmailAddress: from,
          Destination: {
            ToAddresses: Array.isArray(to) ? to : [to],
          },
          Content: {
            Simple: {
              Subject: {
                Data: subject,
                Charset: "UTF-8",
              },
              Body: {
                Html: args.html
                  ? {
                      Data: args.html,
                      Charset: "UTF-8",
                    }
                  : undefined,
                Text: {
                  Data: text,
                  Charset: "UTF-8",
                },
              },
            },
          },
          ConfigurationSetName: process.env.SES_CONFIGURATION_SET_NAME,
        });

        const response = await sesClient.send(command);
        if (!response.MessageId) {
          throw new Error("No MessageId returned from SES");
        }
        return response.MessageId;
      },
    );
    return emailId;
  },
});
