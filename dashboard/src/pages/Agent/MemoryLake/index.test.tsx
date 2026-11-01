import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
const { api } = vi.hoisted(() => ({
  api: {
    status: vi.fn(),
    bindings: vi.fn(),
    createBinding: vi.fn(),
    writePolicy: vi.fn(),
    upload: vi.fn(),
    discardUpload: vi.fn(),
    review: vi.fn(),
    submit: vi.fn(),
    submission: vi.fn(),
  },
}));
vi.mock("../../../api/modules/memorylake", () => ({ memorylakeApi: api }));
vi.mock("../../../context/AgentContext", () => ({
  useAgent: () => ({ activeAgentId: "agent", activeAgent: { id: "agent" } }),
}));
vi.mock("../../../layouts/PageShell", () => ({
  default: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));
import MemoryLake from "./index";

beforeEach(() => {
  vi.clearAllMocks();
  api.status.mockResolvedValue({
    enabled: true,
    available: true,
    max_file_bytes: 100,
    upload_ttl_seconds: 300,
  });
  api.bindings.mockResolvedValue({
    bindings: [
      {
        binding_id: "binding",
        connector_id: "connector",
        workspace_id: "workspace",
        project_id: "project",
        library_parent_id: "folder",
        writes_enabled: true,
        policy_revision: 1,
      },
    ],
  });
  api.upload.mockResolvedValue({ upload_id: "opaque" });
  api.review.mockResolvedValue({
    review_id: "review",
    submission_id: "submission",
    filename: "synthetic.txt",
    size: 3,
    sha256: "a".repeat(64),
    workspace_id: "workspace",
    project_id: "project",
    library_parent_id: "folder",
    expires_at: Date.now() / 1000 + 300,
  });
  api.submit.mockResolvedValue({
    submission_id: "submission",
    state: "processing",
    requires_reconciliation: false,
  });
});
it("keeps deployment disabled without binding or upload calls", async () => {
  api.status.mockResolvedValue({ enabled: false, available: false });
  render(<MemoryLake />);
  expect(await screen.findByText("memorylake.disabled")).toBeInTheDocument();
  expect(api.bindings).not.toHaveBeenCalled();
  expect(api.upload).not.toHaveBeenCalled();
});
async function reviewFile() {
  render(<MemoryLake />);
  const input = await screen.findByLabelText("memorylake.file");
  fireEvent.change(input, {
    target: { files: [new File(["abc"], "synthetic.txt")] },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.review" }));
  await screen.findByText("a".repeat(64));
}
it("uploads only to local snapshot then requires explicit confirmation", async () => {
  await reviewFile();
  expect(api.submit).not.toHaveBeenCalled();
  expect(screen.getByText("workspace / project / folder")).toBeInTheDocument();
  const submit = screen.getByRole("button", { name: "memorylake.submit" });
  expect(submit).toBeDisabled();
  fireEvent.click(screen.getByLabelText("memorylake.confirm"));
  fireEvent.click(submit);
  await waitFor(() => expect(api.submit).toHaveBeenCalledTimes(1));
  expect(api.submit.mock.calls[0].slice(0, 4)).toEqual([
    "agent",
    "binding",
    "opaque",
    "review",
  ]);
  expect(api.submission).not.toHaveBeenCalled();
});
it("uncertain submit never automatically retries or reuploads", async () => {
  api.submit.mockRejectedValue(new Error("secret must not render"));
  await reviewFile();
  fireEvent.click(screen.getByLabelText("memorylake.confirm"));
  fireEvent.click(screen.getByRole("button", { name: "memorylake.submit" }));
  expect(await screen.findByText("memorylake.unknown")).toBeInTheDocument();
  expect(screen.queryByText(/secret must/)).not.toBeInTheDocument();
  expect(api.upload).toHaveBeenCalledTimes(1);
  expect(api.submit).toHaveBeenCalledTimes(1);
  expect(
    screen.queryByRole("button", { name: "memorylake.submit" }),
  ).not.toBeInTheDocument();
});

it("only reads status on explicit refresh and stops immediately at unknown", async () => {
  api.submission.mockResolvedValue({
    submission_id: "submission",
    state: "unknown",
    requires_reconciliation: true,
  });
  await reviewFile();
  fireEvent.click(screen.getByLabelText("memorylake.confirm"));
  fireEvent.click(screen.getByRole("button", { name: "memorylake.submit" }));
  const refresh = await screen.findByRole("button", {
    name: "memorylake.refresh",
  });
  expect(api.submission).not.toHaveBeenCalled();
  fireEvent.click(refresh);
  expect(await screen.findByText("memorylake.unknown")).toBeInTheDocument();
  expect(api.submission).toHaveBeenCalledTimes(1);
  expect(api.submit).toHaveBeenCalledTimes(1);
  expect(api.upload).toHaveBeenCalledTimes(1);
});

it("clears API key input on save and never persists it", async () => {
  api.createBinding.mockResolvedValue({
    binding_id: "new",
    connector_id: "new",
    workspace_id: "w",
    project_id: "p",
    library_parent_id: "f",
    writes_enabled: true,
  });
  const storage = vi.spyOn(Storage.prototype, "setItem");
  render(<MemoryLake />);
  await screen.findByLabelText("memorylake.apiKey");
  for (const [label, value] of [
    ["apiKey", "synthetic-secret"],
    ["workspace_id", "w"],
    ["project_id", "p"],
    ["library_parent_id", "f"],
  ]) {
    fireEvent.change(screen.getByLabelText(`memorylake.${label}`), {
      target: { value },
    });
  }
  fireEvent.click(
    screen.getByRole("button", { name: "memorylake.saveConnection" }),
  );
  await waitFor(() => expect(api.createBinding).toHaveBeenCalledTimes(1));
  expect(screen.getByLabelText("memorylake.apiKey")).toHaveValue("");
  expect(api.createBinding.mock.calls[0][1].writes_enabled).toBe(false);
  expect(storage).not.toHaveBeenCalled();
  storage.mockRestore();
});

it("bounds manual polling to five read requests without replaying a write", async () => {
  api.submission.mockResolvedValue({
    submission_id: "submission",
    state: "processing",
    requires_reconciliation: false,
  });
  await reviewFile();
  fireEvent.click(screen.getByLabelText("memorylake.confirm"));
  fireEvent.click(screen.getByRole("button", { name: "memorylake.submit" }));
  const refresh = await screen.findByRole("button", {
    name: "memorylake.refresh",
  });
  vi.useFakeTimers();
  try {
    fireEvent.click(refresh);
    for (let i = 0; i < 6; i++)
      await act(async () => {
        await vi.advanceTimersByTimeAsync(2000);
      });
    expect(api.submission).toHaveBeenCalledTimes(5);
    expect(api.submit).toHaveBeenCalledTimes(1);
    expect(api.upload).toHaveBeenCalledTimes(1);
  } finally {
    vi.useRealTimers();
  }
});

it("reconciles a known submission ID without creating a file or approval", async () => {
  api.submission.mockResolvedValue({
    submission_id: "existing",
    state: "ready",
    requires_reconciliation: false,
  });
  render(<MemoryLake />);
  const input = await screen.findByLabelText("memorylake.existingId");
  fireEvent.change(input, { target: { value: "existing" } });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.reconcile" }));
  await waitFor(() => expect(api.submission).toHaveBeenCalledTimes(1));
  expect(api.submission.mock.calls[0].slice(0, 3)).toEqual([
    "agent",
    "binding",
    "existing",
  ]);
  expect(api.upload).not.toHaveBeenCalled();
  expect(api.review).not.toHaveBeenCalled();
  expect(api.submit).not.toHaveBeenCalled();
});

it("pauses writes using current revision without disabling read reconciliation", async () => {
  api.writePolicy.mockResolvedValue({
    binding_id: "binding",
    connector_id: "connector",
    workspace_id: "workspace",
    project_id: "project",
    library_parent_id: "folder",
    writes_enabled: false,
    policy_revision: 2,
  });
  render(<MemoryLake />);
  fireEvent.click(
    await screen.findByRole("button", { name: "memorylake.pauseWrites" }),
  );
  expect(
    await screen.findByText("memorylake.writeDisabled"),
  ).toBeInTheDocument();
  expect(api.writePolicy.mock.calls[0].slice(0, 4)).toEqual([
    "agent",
    "binding",
    false,
    1,
  ]);
  expect(screen.getByLabelText("memorylake.file")).toBeDisabled();
  expect(screen.getByLabelText("memorylake.existingId")).toBeEnabled();
  fireEvent.change(screen.getByLabelText("memorylake.query"), {
    target: { value: "read while writes disabled" },
  });
  expect(
    screen.getByRole("button", { name: "memorylake.search" }),
  ).toBeEnabled();
});

it("aborts local work on unmount and never reviews or submits a late upload", async () => {
  let finish!: (value: { upload_id: string }) => void;
  api.upload.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = render(<MemoryLake />);
  fireEvent.change(await screen.findByLabelText("memorylake.file"), {
    target: { files: [new File(["abc"], "synthetic.txt")] },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.review" }));
  await waitFor(() => expect(api.upload).toHaveBeenCalledTimes(1));
  const signal = api.upload.mock.calls[0][3];
  view.unmount();
  expect(signal.aborted).toBe(true);
  await act(async () => {
    finish({ upload_id: "late" });
  });
  expect(api.review).not.toHaveBeenCalled();
  expect(api.submit).not.toHaveBeenCalled();
});

it("refuses oversize files before creating a server snapshot", async () => {
  render(<MemoryLake />);
  fireEvent.change(await screen.findByLabelText("memorylake.file"), {
    target: { files: [new File(["x".repeat(101)], "large.txt")] },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.review" }));
  expect(await screen.findByText("memorylake.fileSize")).toBeInTheDocument();
  expect(api.upload).not.toHaveBeenCalled();
});

it("allows a new file only after ready, without automatically uploading it", async () => {
  api.submit.mockResolvedValue({
    submission_id: "submission",
    state: "ready",
    requires_reconciliation: false,
  });
  await reviewFile();
  fireEvent.click(screen.getByLabelText("memorylake.confirm"));
  fireEvent.click(screen.getByRole("button", { name: "memorylake.submit" }));
  fireEvent.click(
    await screen.findByRole("button", { name: "memorylake.newFile" }),
  );
  expect(screen.getByLabelText("memorylake.file")).toBeEnabled();
  expect(screen.getByLabelText("memorylake.file")).toHaveValue("");
  expect(api.upload).toHaveBeenCalledTimes(1);
  expect(
    screen.getByRole("button", { name: "memorylake.review" }),
  ).toBeDisabled();
});

it.each(["failure", "cancel", "unknown response"])(
  "retains the planned recovery ID after submit %s without replay",
  async (outcome) => {
    if (outcome === "unknown response")
      api.submit.mockResolvedValue({ state: "unknown" });
    else
      api.submit.mockRejectedValue(
        outcome === "cancel"
          ? new DOMException("Aborted", "AbortError")
          : new Error("synthetic-private-error"),
      );
    api.submission.mockResolvedValue({
      submission_id: "submission",
      state: "processing",
      requires_reconciliation: false,
    });
    await reviewFile();
    expect(screen.getByLabelText("memorylake.existingId")).toHaveValue(
      "submission",
    );
    expect(
      screen.getByText("memorylake.plannedSubmission"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText("memorylake.confirm"));
    fireEvent.click(screen.getByRole("button", { name: "memorylake.submit" }));
    await screen.findByText("memorylake.unknown");
    expect(screen.getByLabelText("memorylake.existingId")).toHaveValue(
      "submission",
    );
    api.submission.mockResolvedValue({
      submission_id: "submission",
      state: "ready",
      requires_reconciliation: false,
    });
    fireEvent.click(screen.getByRole("button", { name: "memorylake.refresh" }));
    await waitFor(() => expect(api.submission).toHaveBeenCalledTimes(1));
    expect(api.submission.mock.calls[0].slice(0, 3)).toEqual([
      "agent",
      "binding",
      "submission",
    ]);
    expect(api.upload).toHaveBeenCalledTimes(1);
    expect(api.submit).toHaveBeenCalledTimes(1);
  },
);

it("retains an uploaded snapshot for explicit discard when review fails", async () => {
  api.review.mockRejectedValue(new Error("synthetic-private-error"));
  api.discardUpload
    .mockRejectedValueOnce(new Error("temporary failure"))
    .mockResolvedValue({ discarded: true });
  render(<MemoryLake />);
  fireEvent.change(await screen.findByLabelText("memorylake.file"), {
    target: { files: [new File(["abc"], "synthetic.txt")] },
  });
  fireEvent.click(screen.getByRole("button", { name: "memorylake.review" }));
  const discard = await screen.findByRole("button", {
    name: "memorylake.discard",
  });
  await waitFor(() => expect(discard).toBeEnabled());
  fireEvent.click(discard);
  await waitFor(() => expect(api.discardUpload).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(discard).toBeEnabled());
  expect(
    screen.getByRole("button", { name: "memorylake.discard" }),
  ).toBeInTheDocument();
  fireEvent.click(discard);
  await waitFor(() =>
    expect(
      screen.queryByRole("button", { name: "memorylake.discard" }),
    ).not.toBeInTheDocument(),
  );
  expect(api.discardUpload.mock.calls[0].slice(0, 3)).toEqual([
    "agent",
    "binding",
    "opaque",
  ]);
  expect(api.upload).toHaveBeenCalledTimes(1);
  expect(api.submit).not.toHaveBeenCalled();
});

it("does not offer local discard after submission begins even if result is unknown", async () => {
  api.submit.mockRejectedValue(new Error("uncertain"));
  await reviewFile();
  expect(
    screen.getByRole("button", { name: "memorylake.discard" }),
  ).toBeInTheDocument();
  fireEvent.click(screen.getByLabelText("memorylake.confirm"));
  fireEvent.click(screen.getByRole("button", { name: "memorylake.submit" }));
  await screen.findByText("memorylake.unknown");
  expect(
    screen.queryByRole("button", { name: "memorylake.discard" }),
  ).not.toBeInTheDocument();
  expect(api.discardUpload).not.toHaveBeenCalled();
});

it("retries only review for an existing snapshot and permits discard after write opt-out", async () => {
  api.review.mockRejectedValueOnce(new Error("review failed"));
  api.writePolicy.mockResolvedValue({
    binding_id: "binding",
    connector_id: "connector",
    workspace_id: "workspace",
    project_id: "project",
    library_parent_id: "folder",
    writes_enabled: false,
    policy_revision: 2,
  });
  api.discardUpload.mockResolvedValue({ discarded: true });
  render(<MemoryLake />);
  const input = await screen.findByLabelText("memorylake.file");
  fireEvent.change(input, {
    target: { files: [new File(["abc"], "synthetic.txt")] },
  });
  const button = screen.getByRole("button", { name: "memorylake.review" });
  fireEvent.click(button);
  await screen.findByText("memorylake.error");
  expect(input).toBeDisabled();
  fireEvent.click(button);
  await screen.findByText("a".repeat(64));
  expect(api.upload).toHaveBeenCalledTimes(1);
  expect(api.review).toHaveBeenCalledTimes(2);
  fireEvent.click(
    screen.getByRole("button", { name: "memorylake.pauseWrites" }),
  );
  await screen.findByText("memorylake.writeDisabled");
  const discard = screen.getByRole("button", { name: "memorylake.discard" });
  expect(discard).toBeEnabled();
  fireEvent.click(discard);
  await waitFor(() => expect(api.discardUpload).toHaveBeenCalledTimes(1));
  expect(api.submit).not.toHaveBeenCalled();
});

it("does not display a late ready result after stopping a read-only poll", async () => {
  let finish!: (value: unknown) => void;
  api.submission.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  await reviewFile();
  fireEvent.click(screen.getByLabelText("memorylake.confirm"));
  fireEvent.click(screen.getByRole("button", { name: "memorylake.submit" }));
  const refresh = await screen.findByRole("button", {
    name: "memorylake.refresh",
  });
  await waitFor(() => expect(refresh).toBeEnabled());
  fireEvent.click(refresh);
  await waitFor(() => expect(api.submission).toHaveBeenCalledTimes(1));
  const signal = api.submission.mock.calls[0][3];
  fireEvent.click(
    screen.getByRole("button", { name: "memorylake.stopWaiting" }),
  );
  expect(signal.aborted).toBe(true);
  await act(async () => {
    finish({
      submission_id: "submission",
      state: "ready",
      requires_reconciliation: false,
    });
  });
  expect(
    screen.queryByRole("button", { name: "memorylake.newFile" }),
  ).not.toBeInTheDocument();
  expect(screen.getByText(/memorylake.states.processing/)).toBeInTheDocument();
});
