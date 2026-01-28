import { expect, describe, it, beforeEach } from "vitest";
import { api } from "./_generated/api.js";
import type { EmailEvent } from "./shared.js";
import {
  createTestEventOfType,
  insertTestSentEmail,
  setupTest,
  setupTestLastOptions,
  type Tester,
} from "./setup.test.js";
import { type Doc, type Id } from "./_generated/dataModel.js";

describe("handleEmailEvent", () => {
  let t: Tester;
  let event: EmailEvent;
  let email: Doc<"emails">;

  beforeEach(async () => {
    t = setupTest();
    event = createTestEventOfType("Delivery");
    await setupTestLastOptions(t);
    email = await insertTestSentEmail(t);
  });

  const exec = async (_event: EmailEvent | unknown = event) => {
    await t.mutation(api.lib.handleEmailEvent, { event: _event });
  };

  const getEmail = () =>
    t.run(async (ctx) => {
      const _email = await ctx.db.get(email._id);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

  it("updates email for delivered event", async () => {
    expect(email.status).toBe("sent");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("delivered");
    expect(updatedEmail.finalizedAt).toBeLessThan(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.finalizedAt).toBeGreaterThan(Date.now() - 10000); // Within last 10 seconds
    // deliveryEvents entry created
    const events = await t.run(async (ctx) =>
      ctx.db
        .query("deliveryEvents")
        .withIndex("by_emailId_eventType", (q) =>
          q.eq("emailId", email._id).eq("eventType", "Delivery"),
        )
        .collect(),
    );
    expect(events.length).toBe(1);
    expect(events[0].eventType).toBe("Delivery");
    expect(events[0].sesMessageId).toBe("test-ses-message-id-123");
  });

  it("updates email for complained event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("Complaint");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.complained).toBe(true);
    // deliveryEvents entry created
    const events = await t.run(async (ctx) =>
      ctx.db
        .query("deliveryEvents")
        .withIndex("by_emailId_eventType", (q) => q.eq("emailId", email._id))
        .collect(),
    );
    expect(events.length).toBe(1);
    expect(events[0].eventType).toBe("Complaint");
  });

  it("updates email for bounced event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("Bounce");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("bounced");
    expect(updatedEmail.finalizedAt).toBeLessThan(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.finalizedAt).toBeGreaterThan(Date.now() - 10000); // Within last 10 seconds
    expect(updatedEmail.errorMessage).toContain(
      "Permanent/General: recipient@example.com: The email bounced due to invalid recipient",
    );
    // deliveryEvents entry created
    const events = await t.run(async (ctx) =>
      ctx.db
        .query("deliveryEvents")
        .withIndex("by_emailId_eventType", (q) =>
          q.eq("emailId", email._id).eq("eventType", "Bounce"),
        )
        .collect(),
    );
    expect(events.length).toBe(1);
    expect(events[0].eventType).toBe("Bounce");
    expect(events[0].message).toBe(
      "The email bounced due to invalid recipient",
    );
  });

  it("updates email for delivery_delayed event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("DeliveryDelay");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("delivery_delayed");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER); // Should remain unchanged
  });

  it("updates email for opened event", async () => {
    expect(email.status).toBe("sent");
    expect(email.opened).toBe(false);
    event = createTestEventOfType("Open");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.opened).toBe(true);
  });

  it("does not update email for sent event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("Send");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER); // Should remain unchanged
    expect(updatedEmail.complained).toBe(false); // Should remain unchanged
    expect(updatedEmail.opened).toBe(false); // Should remain unchanged
  });

  it("updates email for clicked event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("Click");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER); // Should remain unchanged
    expect(updatedEmail.clicked).toBe(true); // Now tracks clicks
    expect(updatedEmail.complained).toBe(false); // Should remain unchanged
    expect(updatedEmail.opened).toBe(false); // Should remain unchanged
  });

  it("updates email for reject event and changes status", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("Reject");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("failed"); // Status changes (failed has higher priority than sent)
    expect(updatedEmail.failed).toBe(true); // Flag is set
    expect(updatedEmail.finalizedAt).toBeLessThan(Number.MAX_SAFE_INTEGER); // Should be finalized
    expect(updatedEmail.complained).toBe(false); // Should remain unchanged
    expect(updatedEmail.opened).toBe(false); // Should remain unchanged
  });

  it("gracefully handles invalid event structure - missing eventType", async () => {
    const invalidEvent = {
      mail: {
        timestamp: "2024-01-01T00:00:00Z",
        messageId: "test-ses-message-id-123",
        source: "test@example.com",
        destination: ["recipient@example.com"],
      },
    };

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles invalid event structure - missing mail", async () => {
    const invalidEvent = {
      eventType: "Delivery",
    };

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles completely invalid event", async () => {
    const invalidEvent = "not an object";

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles null event", async () => {
    // Should not throw an error
    await exec(null);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles empty object event", async () => {
    const invalidEvent = {};

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });
});

describe("sendEmail with templates", () => {
  let t: Tester;

  beforeEach(async () => {
    t = setupTest();
    await setupTestLastOptions(t);
  });

  it("should accept template-based email", async () => {
    const emailId: Id<"emails"> = await t.mutation(api.lib.sendEmail, {
      options: {
        region: "us-east-1",
        accessKeyId: "test-access-key-id",
        secretAccessKey: "test-secret-access-key",
        initialBackoffMs: 1000,
        retryAttempts: 3,
        testMode: true,
      },
      from: "test@example.com",
      to: ["recipient@example.com"],
      template: {
        name: "order-confirmation",
        data: {
          PRODUCT: "Vintage Macintosh",
          PRICE: 499,
        },
      },
    });

    const email = await t.run(async (ctx) => {
      const _email = await ctx.db.get(emailId);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

    expect(email.template?.name).toBe("order-confirmation");
    expect(email.template?.data).toEqual({
      PRODUCT: "Vintage Macintosh",
      PRICE: 499,
    });
    expect(email.subject).toBeUndefined();
    expect(email.html).toBeUndefined();
    expect(email.text).toBeUndefined();
    expect(email.status).toBe("waiting");
  });

  it("should reject email with both template and html/text", async () => {
    await expect(
      t.mutation(api.lib.sendEmail, {
        options: {
          region: "us-east-1",
          accessKeyId: "test-access-key-id",
          secretAccessKey: "test-secret-access-key",
          initialBackoffMs: 1000,
          retryAttempts: 3,
          testMode: true,
        },
        from: "test@example.com",
        to: ["recipient@example.com"],
        subject: "Test",
        html: "<p>Test</p>",
        template: {
          name: "order-confirmation",
          data: {
            PRODUCT: "Test",
          },
        },
      }),
    ).rejects.toThrow("Cannot provide both html/text and template");
  });

  it("should accept template email with optional subject", async () => {
    const emailId: Id<"emails"> = await t.mutation(api.lib.sendEmail, {
      options: {
        region: "us-east-1",
        accessKeyId: "test-access-key-id",
        secretAccessKey: "test-secret-access-key",
        initialBackoffMs: 1000,
        retryAttempts: 3,
        testMode: true,
      },
      from: "test@example.com",
      to: ["recipient@example.com"],
      subject: "Custom Subject Override",
      template: {
        name: "order-confirmation",
        data: {
          PRODUCT: "Test",
        },
      },
    });

    const email = await t.run(async (ctx) => {
      const _email = await ctx.db.get(emailId);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

    expect(email.template?.name).toBe("order-confirmation");
    expect(email.template?.data).toEqual({
      PRODUCT: "Test",
    });
    expect(email.subject).toBe("Custom Subject Override");
    expect(email.status).toBe("waiting");
  });

  it("should reject email without content or template", async () => {
    await expect(
      t.mutation(api.lib.sendEmail, {
        options: {
          region: "us-east-1",
          accessKeyId: "test-access-key-id",
          secretAccessKey: "test-secret-access-key",
          initialBackoffMs: 1000,
          retryAttempts: 3,
          testMode: true,
        },
        from: "test@example.com",
        to: ["recipient@example.com"],
      }),
    ).rejects.toThrow("Either html/text or template must be provided");
  });

  it("should reject traditional email without subject", async () => {
    await expect(
      t.mutation(api.lib.sendEmail, {
        options: {
          region: "us-east-1",
          accessKeyId: "test-access-key-id",
          secretAccessKey: "test-secret-access-key",
          initialBackoffMs: 1000,
          retryAttempts: 3,
          testMode: true,
        },
        from: "test@example.com",
        to: ["recipient@example.com"],
        html: "<p>Test</p>",
      }),
    ).rejects.toThrow("Subject is required when not using a template");
  });
});
