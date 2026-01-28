import { describe, it, expect } from "vitest";
import { attemptToParse } from "./utils.js";
import { v } from "convex/values";

describe("attemptToParse", () => {
  it("returns success for valid data", () => {
    const validator = v.object({
      name: v.string(),
      age: v.number(),
    });
    const result = attemptToParse(validator, { name: "test", age: 25 });
    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      expect(result.data).toEqual({ name: "test", age: 25 });
    }
  });

  it("returns error for invalid data", () => {
    const validator = v.object({
      name: v.string(),
      age: v.number(),
    });
    const result = attemptToParse(validator, { name: "test", age: "not a number" });
    expect(result.kind).toBe("error");
  });

  it("returns error for null", () => {
    const validator = v.object({
      name: v.string(),
    });
    const result = attemptToParse(validator, null);
    expect(result.kind).toBe("error");
  });

  it("returns error for undefined", () => {
    const validator = v.object({
      name: v.string(),
    });
    const result = attemptToParse(validator, undefined);
    expect(result.kind).toBe("error");
  });
});
