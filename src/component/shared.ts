import { literals } from "convex-helpers/validators";
import {
  type GenericDataModel,
  type GenericMutationCtx,
  type GenericQueryCtx,
} from "convex/server";
import { type Infer, v } from "convex/values";

// Validator for the onEmailEvent option.
export const onEmailEvent = v.object({
  fnHandle: v.string(),
});

// Validator for the status of an email.
export const vStatus = v.union(
  v.literal("waiting"),
  v.literal("queued"),
  v.literal("cancelled"),
  v.literal("sent"),
  v.literal("delivered"),
  v.literal("delivery_delayed"),
  v.literal("bounced"),
  v.literal("failed"),
);
export type Status = Infer<typeof vStatus>;

// Validator for template data (SES templates).
export const vTemplate = v.object({
  name: v.string(),
  data: v.optional(v.record(v.string(), v.union(v.string(), v.number()))),
});
export type Template = Infer<typeof vTemplate>;

// Validator for the runtime options used by the component.
export const vOptions = v.object({
  initialBackoffMs: v.number(),
  retryAttempts: v.number(),
  // AWS SES configuration
  region: v.string(),
  accessKeyId: v.string(),
  secretAccessKey: v.string(),
  // Optional: Configuration set name for event publishing
  configurationSetName: v.optional(v.string()),
  // Test mode (for SES sandbox)
  testMode: v.boolean(),
  onEmailEvent: v.optional(onEmailEvent),
});

export type RuntimeConfig = Infer<typeof vOptions>;

// SES SNS notification event types
export const vSesEventType = v.union(
  v.literal("Send"),
  v.literal("Delivery"),
  v.literal("Bounce"),
  v.literal("Complaint"),
  v.literal("Reject"),
  v.literal("Open"),
  v.literal("Click"),
  v.literal("DeliveryDelay"),
  v.literal("Rendering Failure"),
);

// Mail object present in all SES events
const vSesMail = v.object({
  timestamp: v.string(),
  messageId: v.string(),
  source: v.string(),
  sourceArn: v.optional(v.string()),
  sendingAccountId: v.optional(v.string()),
  destination: v.array(v.string()),
  headersTruncated: v.optional(v.boolean()),
  headers: v.optional(
    v.array(
      v.object({
        name: v.string(),
        value: v.string(),
      }),
    ),
  ),
  commonHeaders: v.optional(
    v.object({
      from: v.optional(v.array(v.string())),
      to: v.optional(v.array(v.string())),
      date: v.optional(v.string()),
      subject: v.optional(v.string()),
      messageId: v.optional(v.string()),
    }),
  ),
  tags: v.optional(v.record(v.string(), v.array(v.string()))),
});

// Bounce details
const vSesBounce = v.object({
  bounceType: v.string(),
  bounceSubType: v.string(),
  bouncedRecipients: v.array(
    v.object({
      emailAddress: v.string(),
      action: v.optional(v.string()),
      status: v.optional(v.string()),
      diagnosticCode: v.optional(v.string()),
    }),
  ),
  timestamp: v.string(),
  feedbackId: v.string(),
  reportingMTA: v.optional(v.string()),
});

// Complaint details
const vSesComplaint = v.object({
  complainedRecipients: v.array(
    v.object({
      emailAddress: v.string(),
    }),
  ),
  timestamp: v.string(),
  feedbackId: v.string(),
  userAgent: v.optional(v.string()),
  complaintFeedbackType: v.optional(v.string()),
  arrivalDate: v.optional(v.string()),
});

// Delivery details
const vSesDelivery = v.object({
  timestamp: v.string(),
  processingTimeMillis: v.number(),
  recipients: v.array(v.string()),
  smtpResponse: v.string(),
  reportingMTA: v.string(),
});

// Open details
const vSesOpen = v.object({
  ipAddress: v.string(),
  timestamp: v.string(),
  userAgent: v.string(),
});

// Click details
const vSesClick = v.object({
  ipAddress: v.string(),
  timestamp: v.string(),
  userAgent: v.string(),
  link: v.string(),
  linkTags: v.optional(v.record(v.string(), v.array(v.string()))),
});

// Reject details
const vSesReject = v.object({
  reason: v.string(),
});

// DeliveryDelay details
const vSesDeliveryDelay = v.object({
  timestamp: v.string(),
  delayType: v.string(),
  expirationTime: v.optional(v.string()),
  delayedRecipients: v.array(
    v.object({
      emailAddress: v.string(),
      status: v.optional(v.string()),
      diagnosticCode: v.optional(v.string()),
    }),
  ),
});

// SES email event from SNS notification
export const vEmailEvent = v.union(
  v.object({
    eventType: v.literal("Send"),
    mail: vSesMail,
  }),
  v.object({
    eventType: v.literal("Delivery"),
    mail: vSesMail,
    delivery: vSesDelivery,
  }),
  v.object({
    eventType: v.literal("Bounce"),
    mail: vSesMail,
    bounce: vSesBounce,
  }),
  v.object({
    eventType: v.literal("Complaint"),
    mail: vSesMail,
    complaint: vSesComplaint,
  }),
  v.object({
    eventType: v.literal("Reject"),
    mail: vSesMail,
    reject: vSesReject,
  }),
  v.object({
    eventType: v.literal("Open"),
    mail: vSesMail,
    open: vSesOpen,
  }),
  v.object({
    eventType: v.literal("Click"),
    mail: vSesMail,
    click: vSesClick,
  }),
  v.object({
    eventType: v.literal("DeliveryDelay"),
    mail: vSesMail,
    deliveryDelay: vSesDeliveryDelay,
  }),
  v.object({
    eventType: v.literal("Rendering Failure"),
    mail: vSesMail,
    failure: v.object({
      errorMessage: v.string(),
      templateName: v.string(),
    }),
  }),
);

export const ACCEPTED_EVENT_TYPES = [
  "Send",
  "Delivery",
  "Bounce",
  "Complaint",
  "Reject",
  "Open",
  "Click",
  "DeliveryDelay",
] as const;

export const vEventType = v.union(literals(...ACCEPTED_EVENT_TYPES));

export type EmailEvent = Infer<typeof vEmailEvent>;
export type EventEventTypes = EmailEvent["eventType"];
export type EventEventOfType<T extends EventEventTypes> = Extract<
  EmailEvent,
  { eventType: T }
>;

/* Type utils follow */

export type RunQueryCtx = {
  runQuery: GenericQueryCtx<GenericDataModel>["runQuery"];
};
export type RunMutationCtx = {
  runMutation: GenericMutationCtx<GenericDataModel>["runMutation"];
};
