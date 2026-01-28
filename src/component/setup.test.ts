/// <reference types="vite/client" />
import { test } from "vitest";
import type {
  EventEventOfType,
  EventEventTypes,
  RuntimeConfig,
} from "./shared.js";
import { convexTest } from "convex-test";
import schema from "./schema.js";
import type { Doc } from "./_generated/dataModel.js";
import { assertExhaustive } from "./utils.js";

export const modules = import.meta.glob("./**/*.*s");

export const setupTest = () => {
  const t = convexTest(schema, modules);
  return t;
};

export type Tester = ReturnType<typeof setupTest>;

test("setup", () => {});

// SES mail object base
const createBaseSesMail = () => ({
  timestamp: "2024-01-01T00:00:00Z",
  messageId: "test-ses-message-id-123",
  source: "test@example.com",
  destination: ["recipient@example.com"],
  headers: [{ name: "X-Test-Header", value: "test-value" }],
  commonHeaders: {
    from: ["test@example.com"],
    to: ["recipient@example.com"],
    subject: "Test Email",
  },
});

export const createTestEventOfType = <T extends EventEventTypes>(
  type: T,
  overrides?: Partial<EventEventOfType<T>>,
): EventEventOfType<T> => {
  const baseMail = createBaseSesMail();

  // Helper to merge overrides with base event
  const applyOverrides = (event: {
    eventType: string;
    mail: typeof baseMail;
    [key: string]: unknown;
  }): EventEventOfType<T> => {
    if (!overrides) return event as EventEventOfType<T>;

    return {
      ...event,
      ...overrides,
      mail: overrides.mail ? { ...event.mail, ...overrides.mail } : event.mail,
    } as EventEventOfType<T>;
  };

  if (type === "Send")
    return applyOverrides({
      eventType: "Send",
      mail: baseMail,
    });

  if (type === "Delivery")
    return applyOverrides({
      eventType: "Delivery",
      mail: baseMail,
      delivery: {
        timestamp: "2024-01-01T00:01:00Z",
        processingTimeMillis: 1500,
        recipients: ["recipient@example.com"],
        smtpResponse: "250 2.0.0 OK",
        reportingMTA: "smtp.example.com",
      },
    });

  if (type === "DeliveryDelay")
    return applyOverrides({
      eventType: "DeliveryDelay",
      mail: baseMail,
      deliveryDelay: {
        timestamp: "2024-01-01T00:01:00Z",
        delayType: "InternalFailure",
        delayedRecipients: [
          {
            emailAddress: "recipient@example.com",
            status: "delayed",
            diagnosticCode: "smtp; 421 Service unavailable",
          },
        ],
      },
    });

  if (type === "Complaint")
    return applyOverrides({
      eventType: "Complaint",
      mail: baseMail,
      complaint: {
        complainedRecipients: [{ emailAddress: "recipient@example.com" }],
        timestamp: "2024-01-01T00:05:00Z",
        feedbackId: "feedback-123",
        complaintFeedbackType: "abuse",
      },
    });

  if (type === "Bounce")
    return applyOverrides({
      eventType: "Bounce",
      mail: baseMail,
      bounce: {
        bounceType: "Permanent",
        bounceSubType: "General",
        bouncedRecipients: [
          {
            emailAddress: "recipient@example.com",
            action: "failed",
            status: "5.1.1",
            diagnosticCode: "The email bounced due to invalid recipient",
          },
        ],
        timestamp: "2024-01-01T00:01:00Z",
        feedbackId: "bounce-feedback-123",
        reportingMTA: "smtp.example.com",
      },
    });

  if (type === "Open")
    return applyOverrides({
      eventType: "Open",
      mail: baseMail,
      open: {
        ipAddress: "192.168.1.100",
        timestamp: "2024-01-01T00:05:00Z",
        userAgent:
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      },
    });

  if (type === "Click")
    return applyOverrides({
      eventType: "Click",
      mail: baseMail,
      click: {
        ipAddress: "192.168.1.100",
        link: "https://example.com/test-link",
        timestamp: "2024-01-01T00:10:00Z",
        userAgent:
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      },
    });

  if (type === "Reject")
    return applyOverrides({
      eventType: "Reject",
      mail: baseMail,
      reject: {
        reason: "SMTP server rejected the email",
      },
    });

  if (type === "Rendering Failure")
    return applyOverrides({
      eventType: "Rendering Failure",
      mail: baseMail,
      failure: {
        errorMessage: "Template rendering failed",
        templateName: "test-template",
      },
    });

  return assertExhaustive(type);
};

export const createTestRuntimeConfig = (): RuntimeConfig => ({
  region: "us-east-1",
  accessKeyId: "test-access-key-id",
  secretAccessKey: "test-secret-access-key",
  testMode: true,
  initialBackoffMs: 1000,
  retryAttempts: 3,
});

export const setupTestLastOptions = (
  t: Tester,
  overrides?: Partial<Doc<"lastOptions">>,
) =>
  t.run(async (ctx) => {
    await ctx.db.insert("lastOptions", {
      options: {
        ...createTestRuntimeConfig(),
      },
      ...overrides,
    });
  });

export const insertTestEmail = (
  t: Tester,
  overrides: Omit<Doc<"emails">, "_id" | "_creationTime">,
) =>
  t.run(async (ctx) => {
    const id = await ctx.db.insert("emails", overrides);
    const email = await ctx.db.get(id);
    if (!email) throw new Error("Email not found");
    return email;
  });

export const insertTestSentEmail = (
  t: Tester,
  overrides?: Partial<Doc<"emails">>,
) =>
  insertTestEmail(t, {
    from: "test@example.com",
    to: "recipient@example.com",
    subject: "Test Email",
    replyTo: [],
    status: "sent",
    bounced: false,
    complained: false,
    failed: false,
    deliveryDelayed: false,
    opened: false,
    clicked: false,
    sesMessageId: "test-ses-message-id-123",
    segment: 1,
    finalizedAt: Number.MAX_SAFE_INTEGER, // FINALIZED_EPOCH
    ...overrides,
  });
