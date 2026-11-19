import { describe, expect, it } from "bun:test";
import { staticPath } from "../serve.js";

const ROOT = "/srv/dist";

describe("staticPath", () => {
  it("serves index.html at the root and files under it", () => {
    expect(staticPath(ROOT, "/")).toBe("/srv/dist/index.html");
    expect(staticPath(ROOT, "/chunk-abc.js")).toBe("/srv/dist/chunk-abc.js");
  });

  it("refuses paths that leave the root, encoded or not", () => {
    expect(staticPath(ROOT, "/../secret")).toBeUndefined();
    expect(staticPath(ROOT, "/..%2f..%2fetc%2fpasswd")).toBeUndefined();
    expect(staticPath(ROOT, "/%2e%2e/dist-other/x")).toBeUndefined();
    expect(staticPath(ROOT, "/%E0%A4%A")).toBeUndefined();
    expect(staticPath(ROOT, "/index.html%00.js")).toBeUndefined();
  });
});
