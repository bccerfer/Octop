import { useEffect, useRef, useState } from "react";
import { Alert, Button, Card, Input, Space } from "antd";
import { useTranslation } from "react-i18next";
import {
  memorylakeApi as api,
  type MemoryLakeSearchResult,
  type MemoryLakeDocumentResult,
} from "../../../api/modules/memorylake";

interface Props {
  agent: string;
  binding: string;
}
// Scope changes destroy state and abort requests, even if the caller forgets a key.
export default function ReadPanel(props: Props) {
  return (
    <ScopedReads
      key={JSON.stringify([props.agent, props.binding])}
      {...props}
    />
  );
}
function ScopedReads({ agent, binding }: Props) {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [topK, setTopK] = useState(5);
  const [results, setResults] = useState<MemoryLakeSearchResult>();
  const [document, setDocument] = useState<MemoryLakeDocumentResult>();
  const [error, setError] = useState(false);
  const [busy, setBusy] = useState(false);
  const generation = useRef(0);
  const pending = useRef<AbortController>();
  const timer = useRef<ReturnType<typeof setTimeout>>();
  useEffect(
    () => () => {
      generation.current++;
      pending.current?.abort();
      clearTimeout(timer.current);
    },
    [],
  );
  const valid =
    query.trim().length > 0 &&
    query.length <= 4000 &&
    !query.includes("\0") &&
    Number.isInteger(topK) &&
    topK >= 1 &&
    topK <= 50;
  async function read(kind: "search" | "document", id?: string) {
    if (kind === "search" && !valid) return;
    if (kind === "document" && !id) return;
    const sequence = ++generation.current;
    pending.current?.abort();
    clearTimeout(timer.current);
    const controller = new AbortController();
    pending.current = controller;
    timer.current = setTimeout(() => controller.abort(), 30000);
    setBusy(true);
    setError(false);
    setDocument(undefined);
    if (kind === "search") setResults(undefined);
    try {
      if (kind === "search") {
        const result = await api.search(
          agent,
          binding,
          query.trim(),
          topK,
          controller.signal,
        );
        if (sequence !== generation.current || controller.signal.aborted)
          return;
        if (
          result.operation !== "search_documents" ||
          result.untrusted !== true
        )
          throw new Error("invalid_read");
        setResults(result);
      } else {
        const result = await api.document(
          agent,
          binding,
          id!,
          controller.signal,
        );
        if (sequence !== generation.current || controller.signal.aborted)
          return;
        if (
          result.operation !== "get_document" ||
          result.untrusted !== true ||
          result.data.id !== id
        )
          throw new Error("invalid_read");
        setDocument(result);
      }
    } catch {
      if (sequence === generation.current) setError(true);
    } finally {
      if (sequence === generation.current) {
        clearTimeout(timer.current);
        setBusy(false);
      }
    }
  }
  return (
    <Card title={t("memorylake.searchTitle")}>
      <Space direction="vertical" style={{ width: "100%" }}>
        <Alert
          type="warning"
          message={t("memorylake.untrusted")}
          description={t("memorylake.readOnlyScope")}
        />
        <label>
          {t("memorylake.query")}
          <Input.TextArea
            aria-label={t("memorylake.query")}
            maxLength={4000}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </label>
        <label>
          {t("memorylake.topK")}
          <input
            aria-label={t("memorylake.topK")}
            type="number"
            min={1}
            max={50}
            step={1}
            value={Number.isFinite(topK) ? topK : ""}
            onChange={(e) => setTopK(Number(e.target.value))}
          />
        </label>
        <Button disabled={!valid} onClick={() => void read("search")}>
          {t("memorylake.search")}
        </Button>
        {busy && <p>{t("memorylake.reading")}</p>}
        {error && <Alert type="error" message={t("memorylake.error")} />}
        {results && (
          <section aria-label={t("memorylake.results")}>
            <p>
              {t("memorylake.requestScope")}: {results.workspace_id} /{" "}
              {results.request_scope.project_ids.join(", ")}
            </p>
            {!results.data.documents.length && (
              <p>{t("memorylake.noResults")}</p>
            )}
            {results.data.documents.map((hit) => (
              <Card
                key={hit.document_id}
                size="small"
                style={{ marginBottom: 12 }}
              >
                <p>
                  {t("memorylake.documentId")}: <code>{hit.document_id}</code>
                </p>
                <p>
                  {t("memorylake.project_id")}:{" "}
                  {hit.project_id ?? t("memorylake.projectUnspecified")}
                </p>
                {hit.document_name && <p>{hit.document_name}</p>}
                {hit.file_name && <p>{hit.file_name}</p>}
                {hit.document_summary && (
                  <p
                    style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
                  >
                    {hit.document_summary}
                  </p>
                )}
                {hit.items?.map((item, index) => (
                  <div
                    key={index}
                    style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
                  >
                    {item.text && <p>{item.text}</p>}
                    {item.highlight?.chunks.map((chunk, part) => (
                      <p key={part}>{chunk.text}</p>
                    ))}
                  </div>
                ))}
                <Button onClick={() => void read("document", hit.document_id)}>
                  {t("memorylake.documentDetails")}
                </Button>
              </Card>
            ))}
          </section>
        )}
        {document && (
          <section aria-label={t("memorylake.documentDetails")}>
            <p>
              {t("memorylake.requestScope")}: {document.workspace_id} /{" "}
              {document.request_scope.project_ids.join(", ")}
            </p>
            <p>
              {t("memorylake.documentId")}: <code>{document.data.id}</code>
            </p>
            {document.data.name && <p>{document.data.name}</p>}
            {document.data.status && <p>{document.data.status}</p>}
            {document.data.document_type && (
              <p>{document.data.document_type}</p>
            )}
            {document.data.created_at && <p>{document.data.created_at}</p>}
          </section>
        )}
      </Space>
    </Card>
  );
}
