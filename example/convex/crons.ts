import { cronJobs } from "convex/server";
import { components, internal } from "./_generated/api.js";
import { internalMutation } from "./_generated/server.js";

const crons = cronJobs();

crons.interval(
  "Remove old emails from the SES component",
  { hours: 1 },
  internal.crons.cleanupSes,
);

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
export const cleanupSes = internalMutation({
  args: {},
  handler: async (ctx) => {
    await ctx.scheduler.runAfter(0, components.ses.lib.cleanupOldEmails, {
      olderThan: ONE_WEEK_MS,
    });
    await ctx.scheduler.runAfter(
      0,
      components.ses.lib.cleanupAbandonedEmails,
      { olderThan: ONE_WEEK_MS },
    );
  },
});

export default crons;
