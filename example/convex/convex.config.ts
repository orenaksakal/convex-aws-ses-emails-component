import { defineApp } from "convex/server";
import ses from "@convex-dev/ses/convex.config";

const app = defineApp();
app.use(ses);

export default app;
