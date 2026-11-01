import { useEffect, useRef, useState } from "react";
import { Alert, Button, Card, Checkbox, Input, Space } from "antd";
import { useTranslation } from "react-i18next";
import { useAgent } from "../../../context/AgentContext";
import PageShell from "../../../layouts/PageShell";
import ReadPanel from "./ReadPanel";
import {
  memorylakeApi as api,
  type MemoryLakeBinding,
  type MemoryLakeConnection,
  type MemoryLakeReview,
  type MemoryLakeStatus,
  type MemoryLakeSubmission,
} from "../../../api/modules/memorylake";

const emptyConnection: MemoryLakeConnection = {
  writes_enabled: false,
  api_key: "",
  base_url: "https://app.memorylake.ai/openapi/memorylake",
  workspace_id: "",
  project_id: "",
  library_parent_id: "",
  display_name: "MemoryLake",
};
export default function MemoryLakePage() {
  const { activeAgentId, activeAgent } = useAgent();
  const { t } = useTranslation();
  return (
    <PageShell
      title="MemoryLake"
      subtitle={t("memorylake.nativeUnchanged")}
      agentScoped
    >
      {activeAgentId && !activeAgent?.bridge ? (
        <FilePanel key={activeAgentId} agent={activeAgentId} />
      ) : (
        <Alert message={t("memorylake.selectAgent")} />
      )}
    </PageShell>
  );
}

function FilePanel({ agent }: { agent: string }) {
  const { t } = useTranslation();
  const [status, setStatus] = useState<MemoryLakeStatus>();
  const [bindings, setBindings] = useState<MemoryLakeBinding[]>([]);
  const [bindingId, setBindingId] = useState("");
  const [connection, setConnection] = useState({ ...emptyConnection });
  const [file, setFile] = useState<File>();
  const [fileInputVersion, setFileInputVersion] = useState(0);
  const [review, setReview] = useState<MemoryLakeReview>();
  const [uploadId, setUploadId] = useState("");
  const [submitStarted, setSubmitStarted] = useState(false);
  const [existingId, setExistingId] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [receipt, setReceipt] = useState<MemoryLakeSubmission>();
  const [unknown, setUnknown] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const alive = useRef(true);
  const operation = useRef<AbortController>();
  useEffect(() => {
    alive.current = true;
    const controller = new AbortController();
    void (async () => {
      try {
        const next = await api.status(controller.signal);
        if (controller.signal.aborted) return;
        setStatus(next);
        if (next.enabled && next.available) {
          const result = await api.bindings(agent, controller.signal);
          if (controller.signal.aborted) return;
          setBindings(result.bindings);
          setBindingId(result.bindings[0]?.binding_id ?? "");
        }
      } catch {
        if (!controller.signal.aborted) setError("memorylake.error");
      }
    })();
    return () => {
      alive.current = false;
      controller.abort();
      operation.current?.abort();
    };
  }, [agent]);
  async function run(
    action: (signal: AbortSignal) => Promise<void>,
    uncertain = false,
  ) {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setError("");
    const controller = new AbortController();
    operation.current = controller;
    // A bounded UI request is not a guarantee the server/remote operation stopped.
    const timeout = setTimeout(() => controller.abort(), 30000);
    try {
      await action(controller.signal);
    } catch {
      if (alive.current) {
        if (uncertain) {
          setUnknown(true);
          setReview(undefined);
        } else setError("memorylake.error");
      }
    } finally {
      clearTimeout(timeout);
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  const selected = bindings.find((b) => b.binding_id === bindingId);
  function resetFile() {
    setFileInputVersion((v) => v + 1);
    setFile(undefined);
    setReview(undefined);
    setUploadId("");
    setSubmitStarted(false);
    setConfirmed(false);
    setReceipt(undefined);
    setUnknown(false);
    setExistingId("");
  }
  async function createConnection() {
    const body = { ...connection };
    // Never keep API keys in drafts, URLs, logs or persistent browser storage.
    setConnection((c) => ({ ...c, api_key: "" }));
    await run(async (signal) => {
      const binding = await api.createBinding(agent, body, signal);
      if (!signal.aborted && alive.current) {
        setBindings((list) => [...list, binding]);
        setBindingId(binding.binding_id);
        resetFile();
      }
    });
  }
  async function changeWritePolicy() {
    if (!selected) return;
    await run(async (signal) => {
      const updated = await api.writePolicy(
        agent,
        bindingId,
        !selected.writes_enabled,
        selected.policy_revision,
        signal,
      );
      if (!signal.aborted && alive.current) {
        setBindings((list) =>
          list.map((b) => (b.binding_id === updated.binding_id ? updated : b)),
        );
        setReview(undefined);
        setConfirmed(false);
      }
    });
  }
  async function prepare() {
    if (!file || !selected?.writes_enabled || !status) return;
    if (file.size === 0 || file.size > status.max_file_bytes) {
      setError("memorylake.fileSize");
      return;
    }
    setReview(undefined);
    setConfirmed(false);
    await run(async (signal) => {
      const id =
        uploadId ||
        (await api.upload(agent, bindingId, file, signal)).upload_id;
      // Retain the local ID even if review fails or waiting is cancelled later.
      if (alive.current) setUploadId(id);
      if (signal.aborted || !alive.current) return;
      const next = await api.review(agent, bindingId, id, signal);
      if (!signal.aborted && alive.current) {
        setUploadId(id);
        setExistingId(next.submission_id);
        setReview(next);
      }
    });
  }
  async function discard() {
    if (!uploadId || submitStarted) return;
    await run(async (signal) => {
      const result = await api.discardUpload(
        agent,
        bindingId,
        uploadId,
        signal,
      );
      if (result?.discarded !== true)
        throw new Error("memorylake_discard_unavailable");
      if (!signal.aborted && alive.current) resetFile();
    });
  }
  async function submit() {
    if (!review || !confirmed || Date.now() / 1000 >= review.expires_at) return;
    setSubmitStarted(true);
    const id = review.review_id;
    const plannedId = review.submission_id;
    setReceipt({
      submission_id: plannedId,
      state: "unknown",
      requires_reconciliation: true,
    });
    setReview(undefined);
    setConfirmed(false);
    await run(async (signal) => {
      const next = await api.submit(agent, bindingId, uploadId, id, signal);
      if (next?.submission_id !== plannedId)
        throw new Error("memorylake_result_unavailable");
      if (alive.current) {
        setReceipt(next);
        setUnknown(
          signal.aborted ||
            next.requires_reconciliation ||
            next.state === "unknown",
        );
      }
    }, true);
  }
  async function poll(id = receipt?.submission_id) {
    if (!id) return;
    setReview(undefined);
    setConfirmed(false);
    await run(async (signal) => {
      const deadline = Date.now() + 30000;
      for (let i = 0; i < 5 && Date.now() < deadline; i++) {
        const next = await api.submission(agent, bindingId, id, signal);
        if (signal.aborted || !alive.current) return;
        setReceipt(next);
        setUnknown(next.requires_reconciliation || next.state === "unknown");
        if (
          ["ready", "failed", "unknown"].includes(next.state) ||
          next.requires_reconciliation
        )
          return;
        if (i < 4)
          await new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, 2000);
            signal.addEventListener(
              "abort",
              () => {
                clearTimeout(timer);
                resolve();
              },
              { once: true },
            );
          });
        if (signal.aborted) return;
      }
    });
  }
  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      {error && <Alert type="error" message={t(error)} />}
      {!status ? (
        <p>{t("memorylake.loading")}</p>
      ) : !status.enabled || !status.available ? (
        <Alert
          message={t("memorylake.disabled")}
          description={t("memorylake.deployment")}
        />
      ) : (
        <>
          <Alert
            type="info"
            message={t("memorylake.localStage")}
            description={t("memorylake.limits", {
              bytes: status.max_file_bytes,
              seconds: status.upload_ttl_seconds,
            })}
          />
          <Card title={t("memorylake.connection")}>
            <Space direction="vertical" style={{ width: "100%" }}>
              <label>
                {t("memorylake.binding")}
                <select
                  aria-label={t("memorylake.binding")}
                  value={bindingId}
                  disabled={busy}
                  onChange={(e) => {
                    setBindingId(e.target.value);
                    resetFile();
                  }}
                >
                  <option value="">—</option>
                  {bindings.map((b) => (
                    <option key={b.binding_id} value={b.binding_id}>
                      {b.workspace_id} / {b.project_id} / {b.binding_id}
                    </option>
                  ))}
                </select>
              </label>
              <details>
                <summary>{t("memorylake.addConnection")}</summary>
                <Space direction="vertical" style={{ width: "100%" }}>
                  <label>
                    {t("memorylake.apiKey")}
                    <Input.Password
                      aria-label={t("memorylake.apiKey")}
                      autoComplete="new-password"
                      value={connection.api_key}
                      onChange={(e) =>
                        setConnection((c) => ({
                          ...c,
                          api_key: e.target.value,
                        }))
                      }
                    />
                  </label>
                  <label>
                    {t("memorylake.endpoint")}
                    <select
                      aria-label={t("memorylake.endpoint")}
                      value={connection.base_url}
                      onChange={(e) =>
                        setConnection((c) => ({
                          ...c,
                          base_url: e.target.value,
                        }))
                      }
                    >
                      <option value="https://app.memorylake.ai/openapi/memorylake">
                        {t("memorylake.global")}
                      </option>
                      <option value="https://app.memorylake.cn/openapi/memorylake">
                        {t("memorylake.china")}
                      </option>
                    </select>
                  </label>
                  {(
                    [
                      "workspace_id",
                      "project_id",
                      "library_parent_id",
                      "display_name",
                    ] as const
                  ).map((key) => (
                    <label key={key}>
                      {t(`memorylake.${key}`)}
                      <Input
                        aria-label={t(`memorylake.${key}`)}
                        value={connection[key]}
                        onChange={(e) =>
                          setConnection((c) => ({
                            ...c,
                            [key]: e.target.value,
                          }))
                        }
                      />
                    </label>
                  ))}
                  <Checkbox
                    checked={connection.writes_enabled}
                    onChange={(e) =>
                      setConnection((c) => ({
                        ...c,
                        writes_enabled: e.target.checked,
                      }))
                    }
                  >
                    {t("memorylake.writeOptIn")}
                  </Checkbox>
                  <Button
                    onClick={() => void createConnection()}
                    disabled={
                      busy ||
                      Object.values(connection).some(
                        (v) => typeof v === "string" && !v.trim(),
                      )
                    }
                  >
                    {t("memorylake.saveConnection")}
                  </Button>
                </Space>
              </details>
            </Space>
          </Card>
          {selected && (
            <Button disabled={busy} onClick={() => void changeWritePolicy()}>
              {t(
                selected.writes_enabled
                  ? "memorylake.pauseWrites"
                  : "memorylake.enableWrites",
              )}
            </Button>
          )}
          {selected && <ReadPanel agent={agent} binding={bindingId} />}
          {selected && !selected.writes_enabled && (
            <Alert message={t("memorylake.writeDisabled")} />
          )}
          {selected && (
            <Card title={t("memorylake.reconcile")}>
              <label>
                {t("memorylake.existingId")}
                <Input
                  aria-label={t("memorylake.existingId")}
                  value={existingId}
                  maxLength={128}
                  disabled={busy}
                  onChange={(e) => setExistingId(e.target.value)}
                />
              </label>
              <Button
                disabled={
                  busy || !/^[A-Za-z0-9][A-Za-z0-9:_-]{0,127}$/.test(existingId)
                }
                onClick={() => void poll(existingId)}
              >
                {t("memorylake.reconcile")}
              </Button>
            </Card>
          )}
          {selected && (
            <Card title={t("memorylake.file")}>
              <Space direction="vertical" style={{ width: "100%" }}>
                <input
                  aria-label={t("memorylake.file")}
                  type="file"
                  key={fileInputVersion}
                  disabled={
                    busy ||
                    !!uploadId ||
                    unknown ||
                    !!receipt ||
                    !selected.writes_enabled
                  }
                  onChange={(e) => {
                    setFile(e.target.files?.[0]);
                    setReview(undefined);
                    setConfirmed(false);
                  }}
                />
                <Button
                  disabled={
                    busy ||
                    !file ||
                    unknown ||
                    !!receipt ||
                    !selected.writes_enabled
                  }
                  onClick={() => void prepare()}
                >
                  {t("memorylake.review")}
                </Button>
                {uploadId && !submitStarted && (
                  <Button disabled={busy} onClick={() => void discard()}>
                    {t("memorylake.discard")}
                  </Button>
                )}
                <p>{t("memorylake.discardScope")}</p>
                {review && (
                  <section aria-label={t("memorylake.reviewDetails")}>
                    <p>
                      <span>{t("memorylake.plannedSubmission")}</span>:{" "}
                      <code>{review.submission_id}</code>
                    </p>
                    <p>
                      {review.filename} ({review.size} {t("memorylake.bytes")})
                    </p>
                    <p>
                      {review.workspace_id} / {review.project_id} /{" "}
                      {review.library_parent_id}
                    </p>
                    <code style={{ overflowWrap: "anywhere" }}>
                      {review.sha256}
                    </code>
                    <p>
                      {t("memorylake.expires")}{" "}
                      {new Date(review.expires_at * 1000).toISOString()}
                    </p>
                    <Checkbox
                      checked={confirmed}
                      onChange={(e) => setConfirmed(e.target.checked)}
                    >
                      {t("memorylake.confirm")}
                    </Checkbox>
                    <Button
                      type="primary"
                      disabled={
                        busy ||
                        !confirmed ||
                        Date.now() / 1000 >= review.expires_at
                      }
                      onClick={() => void submit()}
                    >
                      {t("memorylake.submit")}
                    </Button>
                  </section>
                )}
                {unknown && (
                  <Alert type="warning" message={t("memorylake.unknown")} />
                )}
                {receipt && (
                  <>
                    <p>
                      {t("memorylake.submission")}:{" "}
                      <code>{receipt.submission_id}</code>
                    </p>
                    <p>
                      {t("memorylake.state")}:{" "}
                      {t(
                        `memorylake.states.${
                          [
                            "approved",
                            "uploading",
                            "uploaded",
                            "importing",
                            "processing",
                            "ready",
                            "failed",
                            "unknown",
                          ].includes(receipt.state)
                            ? receipt.state
                            : "unknown"
                        }`,
                      )}
                    </p>
                    <Button disabled={busy} onClick={() => void poll()}>
                      {t("memorylake.refresh")}
                    </Button>
                  </>
                )}
                {receipt?.state === "ready" &&
                  !receipt.requires_reconciliation &&
                  !unknown && (
                    <Button disabled={busy} onClick={resetFile}>
                      {t("memorylake.newFile")}
                    </Button>
                  )}
                {busy && (
                  <Button onClick={() => operation.current?.abort()}>
                    {t("memorylake.stopWaiting")}
                  </Button>
                )}
              </Space>
            </Card>
          )}
        </>
      )}
    </Space>
  );
}
