# Changelog

## 0.3.0

- Complete migration from Resend to AWS SES
- Replaced Resend SDK with AWS SES SDK (@aws-sdk/client-sesv2)
- Replaced webhook handling with SNS notification handling
- Renamed package from @convex-dev/resend to @convex-dev/ses
- Renamed Resend class to Ses class
- Updated environment variables (AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SES_CONFIGURATION_SET_NAME)
- Updated event types for SES format (Delivery, Bounce, Complaint, Open, Click, DeliveryDelay, Reject)
- Changed template support from Resend templates (id/variables) to SES templates (name/data)

## 0.2.3

- Fixed example code for destructuring SDK response
- Fixed receiving webhook responses for emails if you only use the manual
  method

## 0.2.2

- Improved confusing docs which didn't have correct usage for the SDK.

## 0.2.1

- Support for templates and template variables.
- Allows passing multiple recipients in to/cc/bcc.

## 0.2.0

- Adds /test and /\_generated/component.js entrypoints
- Drops commonjs support
- Improves source mapping for generated files
- Changes to a statically generated component API
