import { beforeEach, expect, it, vi } from "vitest";
const { request, requestUpload, getApiUrl } = vi.hoisted(() => ({
  request: vi.fn(),
  requestUpload: vi.fn(),
  getApiUrl: vi.fn(),
}));
vi.mock("../request", () => ({ request, requestUpload }));
vi.mock("../config", () => ({ getApiUrl }));
import { memorylakeApi as api } from "./memorylake";
beforeEach(() => {
  vi.clearAllMocks();
  getApiUrl.mockImplementation((path) => `/api${path}`);
});
it("uses a single file part and explicit review confirmation, never arbitrary paths", () => {
  const file = new File(["synthetic"], "file.txt");
  api.upload("a/b", "binding", file);
  const [path, form] = requestUpload.mock.calls[0];
  expect(path).toBe("/memorylake/agents/a%2Fb/bindings/binding/uploads");
  expect([...form.keys()]).toEqual(["file"]);
  expect(form.get("file")).toBe(file);
  api.review("agent", "binding", "upload");
  expect(request).toHaveBeenLastCalledWith(
    "/memorylake/agents/agent/bindings/binding/uploads/upload/review",
    { method: "POST", signal: undefined },
  );
  api.submit("agent", "binding", "upload", "review");
  expect(request).toHaveBeenLastCalledWith(
    "/memorylake/agents/agent/bindings/binding/uploads/upload/submit",
    {
      method: "POST",
      body: JSON.stringify({ review_id: "review", confirm: true }),
      signal: undefined,
    },
  );
  api.submission("agent", "binding", "submission");
  expect(request).toHaveBeenLastCalledWith(
    "/memorylake/agents/agent/bindings/binding/submissions/submission",
    { signal: undefined },
  );
});
it("refuses cross-origin configuration before touching either transport", () => {
  getApiUrl.mockReturnValue("https://external.invalid/api/memorylake/status");
  expect(() => api.status()).toThrow("memorylake_same_origin_required");
  expect(() => api.upload("a", "b", new File(["x"], "x"))).toThrow(
    "memorylake_same_origin_required",
  );
  expect(request).not.toHaveBeenCalled();
  expect(requestUpload).not.toHaveBeenCalled();
});

it("changes write permission using an explicit revision CAS", () => {
  api.writePolicy("agent", "binding", false, 7);
  expect(request).toHaveBeenCalledWith(
    "/memorylake/agents/agent/bindings/binding/write-policy",
    {
      method: "PATCH",
      body: JSON.stringify({ enabled: false, expected_revision: 7 }),
      signal: undefined,
    },
  );
});

it("discards only the named local snapshot with DELETE", () => {
  api.discardUpload("agent", "binding", "upload");
  expect(request).toHaveBeenCalledWith(
    "/memorylake/agents/agent/bindings/binding/uploads/upload",
    { method: "DELETE", signal: undefined },
  );
});

it("uses fixed binding-scoped read endpoints and explicit top_k", () => {
  api.search("agent", "binding", "query", 5);
  expect(request).toHaveBeenLastCalledWith(
    "/memorylake/agents/agent/bindings/binding/search",
    {
      method: "POST",
      body: JSON.stringify({ query: "query", top_k: 5 }),
      signal: undefined,
    },
  );
  api.document("agent", "binding", "doc");
  expect(request).toHaveBeenLastCalledWith(
    "/memorylake/agents/agent/bindings/binding/documents/doc",
    { signal: undefined },
  );
});
