import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
const { api } = vi.hoisted(() => ({
  api: { search: vi.fn(), document: vi.fn() },
}));
vi.mock("../../../api/modules/memorylake", () => ({ memorylakeApi: api }));
import ReadPanel from "./ReadPanel";
const result = (id = "doc", text = "synthetic text") => ({
  operation: "search_documents",
  request_scope: { project_ids: ["project"] },
  untrusted: true,
  connector_id: "connector",
  workspace_id: "workspace",
  data: {
    documents: [
      {
        document_id: id,
        project_id: "project",
        document_name: "Synthetic",
        document_summary: text,
        private_token: "do-not-show",
        url: "https://external.invalid",
        items: [
          {
            text: "<img src='https://external.invalid/image'>",
            highlight: { chunks: [{ id: "chunk", text: "highlight text" }] },
          },
        ],
      },
    ],
  },
});
beforeEach(() => {
  vi.clearAllMocks();
  api.search.mockResolvedValue(result());
  api.document.mockResolvedValue({
    operation: "get_document",
    request_scope: { project_ids: ["project"] },
    untrusted: true,
    connector_id: "connector",
    workspace_id: "workspace",
    data: {
      id: "doc",
      name: "Synthetic",
      status: "okay",
      private_token: "do-not-show",
    },
  });
});
const props = { agent: "agent", binding: "binding" };
it("searches only on user action and renders external data as inert allowlisted text", async () => {
  const { container } = render(<ReadPanel {...props} />);
  expect(api.search).not.toHaveBeenCalled();
  fireEvent.change(screen.getByLabelText("memorylake.query"), {
    target: { value: "synthetic query" },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.search" }));
  await screen.findByText("synthetic text");
  expect(api.search.mock.calls[0].slice(0, 4)).toEqual([
    "agent",
    "binding",
    "synthetic query",
    5,
  ]);
  expect(screen.getByText("memorylake.untrusted")).toBeInTheDocument();
  expect(screen.getByText("doc")).toBeInTheDocument();
  expect(screen.getByText("highlight text")).toBeInTheDocument();
  expect(container.querySelector("img,a,iframe")).toBeNull();
  expect(screen.queryByText("do-not-show")).not.toBeInTheDocument();
  expect(api.document).not.toHaveBeenCalled();
  fireEvent.click(
    screen.getByRole("button", { name: "memorylake.documentDetails" }),
  );
  await screen.findByText("okay");
  expect(api.document.mock.calls[0].slice(0, 3)).toEqual([
    "agent",
    "binding",
    "doc",
  ]);
});
it("does not display a late response after binding changes", async () => {
  let finish!: (value: unknown) => void;
  api.search.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = render(<ReadPanel {...props} />);
  fireEvent.change(screen.getByLabelText("memorylake.query"), {
    target: { value: "old query" },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.search" }));
  const signal = api.search.mock.calls[0][4];
  view.rerender(<ReadPanel agent="agent" binding="other" />);
  expect(signal.aborted).toBe(true);
  await act(async () => {
    finish(result("stale", "stale-secret-text"));
  });
  expect(screen.queryByText("stale-secret-text")).not.toBeInTheDocument();
});
it("new search aborts old request and only newest result is displayed", async () => {
  let finish!: (value: unknown) => void;
  api.search
    .mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    )
    .mockResolvedValueOnce(result("new", "fresh text"));
  render(<ReadPanel {...props} />);
  fireEvent.change(screen.getByLabelText("memorylake.query"), {
    target: { value: "one" },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.search" }));
  fireEvent.change(screen.getByLabelText("memorylake.query"), {
    target: { value: "two" },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.search" }));
  await screen.findByText("fresh text");
  expect(api.search.mock.calls[0][4].aborted).toBe(true);
  await act(async () => {
    finish(result("old", "stale text"));
  });
  expect(screen.queryByText("stale text")).not.toBeInTheDocument();
});
it("aborts on unmount and hides transport error bodies without retry", async () => {
  api.search.mockRejectedValue(new Error("sensitive upstream body"));
  const view = render(<ReadPanel {...props} />);
  fireEvent.change(screen.getByLabelText("memorylake.query"), {
    target: { value: "query" },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.search" }));
  await waitFor(() =>
    expect(screen.getByText("memorylake.error")).toBeInTheDocument(),
  );
  expect(screen.queryByText(/sensitive upstream/)).not.toBeInTheDocument();
  expect(api.search).toHaveBeenCalledTimes(1);
  view.unmount();
  expect(api.search.mock.calls[0][4].aborted).toBe(true);
});

it("does not collide scoped identities containing colon separators", async () => {
  let finish!: (value: unknown) => void;
  api.search.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = render(<ReadPanel agent="agent:a" binding="b" />);
  fireEvent.change(screen.getByLabelText("memorylake.query"), {
    target: { value: "old query" },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.search" }));
  const signal = api.search.mock.calls[0][4];
  view.rerender(<ReadPanel agent="agent" binding="a:b" />);
  expect(signal.aborted).toBe(true);
  await act(async () => {
    finish(result("stale", "wrong-scope-text"));
  });
  expect(screen.queryByText("wrong-scope-text")).not.toBeInTheDocument();
});

it("enforces local query and result-count limits without issuing a request", () => {
  render(<ReadPanel {...props} />);
  const query = screen.getByLabelText("memorylake.query");
  expect(query).toHaveAttribute("maxlength", "4000");
  for (const [text, count] of [
    ["", 5],
    ["x".repeat(4001), 5],
    ["nul\0query", 5],
    ["query", 0],
    ["query", 51],
    ["query", 1.5],
  ] as const) {
    fireEvent.change(query, { target: { value: text } });
    fireEvent.change(screen.getByLabelText("memorylake.topK"), {
      target: { value: String(count) },
    });
    expect(
      screen.getByRole("button", { name: "memorylake.search" }),
    ).toBeDisabled();
  }
  expect(api.search).not.toHaveBeenCalled();
});
