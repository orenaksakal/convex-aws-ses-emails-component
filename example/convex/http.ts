import { httpRouter } from "convex/server";
import { httpAction } from "./_generated/server";
import { ses } from "./example";

const http = httpRouter();

http.route({
  path: "/ses-notifications",
  method: "POST",
  handler: httpAction(async (ctx, req) => {
    return await ses.handleSnsNotification(ctx, req);
  }),
});

export default http;
