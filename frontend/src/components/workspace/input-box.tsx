"use client";

import type { ChatStatus } from "ai";
import {
  CheckIcon,
  DatabaseIcon,
  GraduationCapIcon,
  LightbulbIcon,
  PaperclipIcon,
  PlusIcon,
  SparklesIcon,
  RocketIcon,
  SearchIcon,
  XIcon,
  ZapIcon,
} from "lucide-react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ComponentProps,
} from "react";

import {
  PromptInput,
  PromptInputActionMenu,
  PromptInputActionMenuContent,
  PromptInputActionMenuItem,
  PromptInputActionMenuTrigger,
  PromptInputAttachment,
  PromptInputAttachments,
  PromptInputBody,
  PromptInputButton,
  PromptInputFooter,
  PromptInputSubmit,
  PromptInputTextarea,
  PromptInputTools,
  usePromptInputAttachments,
  usePromptInputController,
  type PromptInputMessage,
} from "@/components/ai-elements/prompt-input";
import { Button } from "@/components/ui/button";
import { ConfettiButton } from "@/components/ui/confetti-button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenuGroup,
  DropdownMenuLabel,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { getBackendBaseURL } from "@/core/config";
import {
  readSelectedDataSourceIds,
  useDataSources,
  writeSelectedDataSourceIds,
} from "@/core/data-center";
import { useI18n } from "@/core/i18n/hooks";
import { useModels } from "@/core/models/hooks";
import type { ReasoningEffort } from "@/core/threads/reasoning";
import type { AgentThreadContext } from "@/core/threads";
import { textOfMessage } from "@/core/threads/utils";
import { cn } from "@/lib/utils";

import {
  ModelSelector,
  ModelSelectorContent,
  ModelSelectorInput,
  ModelSelectorItem,
  ModelSelectorList,
  ModelSelectorName,
  ModelSelectorTrigger,
} from "../ai-elements/model-selector";
import { Suggestion, Suggestions } from "../ai-elements/suggestion";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "../ui/dropdown-menu";

import { useThread } from "./messages/context";
import { ModeHoverGuide } from "./mode-hover-guide";

type InputMode = "flash" | "thinking" | "pro" | "ultra";

function getResolvedMode(
  mode: InputMode | undefined,
  supportsThinking: boolean,
): InputMode {
  if (!supportsThinking && mode !== "flash") {
    return "flash";
  }
  if (mode) {
    return mode;
  }
  return supportsThinking ? "pro" : "flash";
}

export function InputBox({
  className,
  disabled,
  autoFocus,
  status = "ready",
  context,
  extraHeader,
  isNewThread,
  threadId,
  initialValue,
  onContextChange,
  onSubmit,
  onStop,
  ...props
}: Omit<ComponentProps<typeof PromptInput>, "onSubmit"> & {
  assistantId?: string | null;
  status?: ChatStatus;
  disabled?: boolean;
  context: Omit<
    AgentThreadContext,
    "thread_id" | "is_plan_mode" | "thinking_enabled" | "subagent_enabled"
  > & {
    mode: "flash" | "thinking" | "pro" | "ultra" | undefined;
    reasoning_effort?: ReasoningEffort;
  };
  extraHeader?: React.ReactNode;
  isNewThread?: boolean;
  threadId: string;
  initialValue?: string;
  onContextChange?: (
    context: Omit<
      AgentThreadContext,
      "thread_id" | "is_plan_mode" | "thinking_enabled" | "subagent_enabled"
    > & {
      mode: "flash" | "thinking" | "pro" | "ultra" | undefined;
      reasoning_effort?: ReasoningEffort;
    },
  ) => void;
  onSubmit?: (
    message: PromptInputMessage,
    options?: { extraContext?: Record<string, unknown> },
  ) => void;
  onStop?: () => void;
}) {
  const { t } = useI18n();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [modelDialogOpen, setModelDialogOpen] = useState(false);
  const { models } = useModels();
  const { data: dataSourcesResponse } = useDataSources();
  const { thread, isMock } = useThread();
  const { textInput } = usePromptInputController();
  const promptRootRef = useRef<HTMLDivElement | null>(null);
  const selectionAreaRef = useRef<HTMLDivElement | null>(null);

  const [followups, setFollowups] = useState<string[]>([]);
  const [followupsHidden, setFollowupsHidden] = useState(false);
  const [followupsLoading, setFollowupsLoading] = useState(false);
  const lastGeneratedForAiIdRef = useRef<string | null>(null);
  const wasStreamingRef = useRef(false);

  const [confirmOpen, setConfirmOpen] = useState(false);
  const [pendingSuggestion, setPendingSuggestion] = useState<string | null>(
    null,
  );
  const [dataDialogOpen, setDataDialogOpen] = useState(false);
  const [selectedDataSourceIds, setSelectedDataSourceIds] = useState<string[]>(
    [],
  );
  const [draftSelectedDataSourceIds, setDraftSelectedDataSourceIds] = useState<
    string[]
  >([]);
  const [dataDialogTab, setDataDialogTab] = useState<"sources" | "uploads">(
    "sources",
  );
  const [dataDialogQuery, setDataDialogQuery] = useState("");
  const [selectionAreaHeight, setSelectionAreaHeight] = useState(0);

  useEffect(() => {
    setSelectedDataSourceIds(readSelectedDataSourceIds());
  }, []);

  useEffect(() => {
    writeSelectedDataSourceIds(selectedDataSourceIds);
  }, [selectedDataSourceIds]);

  useEffect(() => {
    if (!dataDialogOpen) {
      return;
    }

    setDraftSelectedDataSourceIds(selectedDataSourceIds);
    setDataDialogQuery("");
    setDataDialogTab("sources");
  }, [dataDialogOpen, selectedDataSourceIds]);

  useEffect(() => {
    if (models.length === 0) {
      return;
    }
    const currentModel = models.find((m) => m.name === context.model_name);
    const fallbackModel = currentModel ?? models[0]!;
    const supportsThinking = fallbackModel.supports_thinking ?? false;
    const nextModelName = fallbackModel.name;
    const nextMode = getResolvedMode(context.mode, supportsThinking);

    if (context.model_name === nextModelName && context.mode === nextMode) {
      return;
    }

    onContextChange?.({
      ...context,
      model_name: nextModelName,
      mode: nextMode,
    });
  }, [context, models, onContextChange]);

  const selectedModel = useMemo(() => {
    if (models.length === 0) {
      return undefined;
    }
    return models.find((m) => m.name === context.model_name) ?? models[0];
  }, [context.model_name, models]);

  const supportThinking = useMemo(
    () => selectedModel?.supports_thinking ?? false,
    [selectedModel],
  );

  const supportReasoningEffort = useMemo(
    () => selectedModel?.supports_reasoning_effort ?? false,
    [selectedModel],
  );

  const handleModelSelect = useCallback(
    (model_name: string) => {
      const model = models.find((m) => m.name === model_name);
      if (!model) {
        return;
      }
      onContextChange?.({
        ...context,
        model_name,
        mode: getResolvedMode(context.mode, model.supports_thinking ?? false),
        reasoning_effort: context.reasoning_effort,
      });
      setModelDialogOpen(false);
    },
    [onContextChange, context, models],
  );

  const handleModeSelect = useCallback(
    (mode: InputMode) => {
      onContextChange?.({
        ...context,
        mode: getResolvedMode(mode, supportThinking),
        reasoning_effort: mode === "ultra" ? "high" : mode === "pro" ? "medium" : mode === "thinking" ? "low" : "minimal",
      });
    },
    [onContextChange, context, supportThinking],
  );

  const handleReasoningEffortSelect = useCallback(
    (effort: ReasoningEffort) => {
      onContextChange?.({
        ...context,
        reasoning_effort: effort,
      });
    },
    [onContextChange, context],
  );

  const selectedDataSources = useMemo(() => {
    return (dataSourcesResponse?.sources ?? []).filter((source) =>
      selectedDataSourceIds.includes(source.id),
    );
  }, [dataSourcesResponse?.sources, selectedDataSourceIds]);

  useEffect(() => {
    if (!selectedDataSourceIds.length) {
      setSelectionAreaHeight(0);
      return;
    }

    const measure = () => {
      setSelectionAreaHeight(selectionAreaRef.current?.offsetHeight ?? 0);
    };

    measure();
    window.addEventListener("resize", measure);
    return () => window.removeEventListener("resize", measure);
  }, [selectedDataSourceIds, selectedDataSources]);

  const attachments = usePromptInputAttachments();

  const dialogSources = useMemo(() => {
    return (dataSourcesResponse?.sources ?? [])
      .filter((source) => source.selectable_in_chat)
      .filter((source) =>
        dataDialogTab === "uploads"
          ? source.type === "uploaded_file"
          : source.type !== "uploaded_file",
      )
      .filter((source) => {
        const haystack = `${source.name} ${source.description ?? ""} ${source.path ?? ""}`;
        return haystack
          .toLowerCase()
          .includes(dataDialogQuery.trim().toLowerCase());
      });
  }, [dataDialogQuery, dataDialogTab, dataSourcesResponse?.sources]);

  const toggleDraftDataSource = useCallback((sourceId: string) => {
    setDraftSelectedDataSourceIds((current) =>
      current.includes(sourceId)
        ? current.filter((id) => id !== sourceId)
        : [...current, sourceId],
    );
  }, []);

  const confirmDataSourceSelection = useCallback(() => {
    setSelectedDataSourceIds(draftSelectedDataSourceIds);
    setDataDialogOpen(false);
  }, [draftSelectedDataSourceIds]);

  const clearDataSourceSelection = useCallback(() => {
    setSelectedDataSourceIds([]);
  }, []);

  const handleSubmit = useCallback(
    async (message: PromptInputMessage) => {
      if (status === "streaming") {
        onStop?.();
        return;
      }
      if (!message.text) {
        return;
      }
      setFollowups([]);
      setFollowupsHidden(false);
      setFollowupsLoading(false);
      onSubmit?.(message, {
        extraContext: {
          selected_data_sources: selectedDataSources.map((source) => ({
            id: source.id,
            name: source.name,
            type: source.type,
            path: source.path,
          })),
        },
      });
    },
    [onSubmit, onStop, selectedDataSources, status],
  );

  const requestFormSubmit = useCallback(() => {
    const form = promptRootRef.current?.querySelector("form");
    form?.requestSubmit();
  }, []);

  const handleFollowupClick = useCallback(
    (suggestion: string) => {
      if (status === "streaming") {
        return;
      }
      const current = (textInput.value ?? "").trim();
      if (current) {
        setPendingSuggestion(suggestion);
        setConfirmOpen(true);
        return;
      }
      textInput.setInput(suggestion);
      setFollowupsHidden(true);
      setTimeout(() => requestFormSubmit(), 0);
    },
    [requestFormSubmit, status, textInput],
  );

  const confirmReplaceAndSend = useCallback(() => {
    if (!pendingSuggestion) {
      setConfirmOpen(false);
      return;
    }
    textInput.setInput(pendingSuggestion);
    setFollowupsHidden(true);
    setConfirmOpen(false);
    setPendingSuggestion(null);
    setTimeout(() => requestFormSubmit(), 0);
  }, [pendingSuggestion, requestFormSubmit, textInput]);

  const confirmAppendAndSend = useCallback(() => {
    if (!pendingSuggestion) {
      setConfirmOpen(false);
      return;
    }
    const current = (textInput.value ?? "").trim();
    const next = current ? `${current}\n${pendingSuggestion}` : pendingSuggestion;
    textInput.setInput(next);
    setFollowupsHidden(true);
    setConfirmOpen(false);
    setPendingSuggestion(null);
    setTimeout(() => requestFormSubmit(), 0);
  }, [pendingSuggestion, requestFormSubmit, textInput]);

  useEffect(() => {
    const streaming = status === "streaming";
    const wasStreaming = wasStreamingRef.current;
    wasStreamingRef.current = streaming;
    if (!wasStreaming || streaming) {
      return;
    }

    if (disabled || isMock) {
      return;
    }

    const lastAi = [...thread.messages].reverse().find((m) => m.type === "ai");
    const lastAiId = lastAi?.id ?? null;
    if (!lastAiId || lastAiId === lastGeneratedForAiIdRef.current) {
      return;
    }
    lastGeneratedForAiIdRef.current = lastAiId;

    const recent = thread.messages
      .filter((m) => m.type === "human" || m.type === "ai")
      .map((m) => {
        const role = m.type === "human" ? "user" : "assistant";
        const content = textOfMessage(m) ?? "";
        return { role, content };
      })
      .filter((m) => m.content.trim().length > 0)
      .slice(-6);

    if (recent.length === 0) {
      return;
    }

    const controller = new AbortController();
    setFollowupsHidden(false);
    setFollowupsLoading(true);
    setFollowups([]);

    fetch(`${getBackendBaseURL()}/api/threads/${threadId}/suggestions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        messages: recent,
        n: 3,
        model_name: context.model_name ?? undefined,
      }),
      signal: controller.signal,
    })
      .then(async (res) => {
        if (!res.ok) {
          return { suggestions: [] as string[] };
        }
        return (await res.json()) as { suggestions?: string[] };
      })
      .then((data) => {
        const suggestions = (data.suggestions ?? [])
          .map((s) => (typeof s === "string" ? s.trim() : ""))
          .filter((s) => s.length > 0)
          .slice(0, 5);
        setFollowups(suggestions);
      })
      .catch(() => {
        setFollowups([]);
      })
      .finally(() => {
        setFollowupsLoading(false);
      });

    return () => controller.abort();
  }, [context.model_name, disabled, isMock, status, thread.messages, threadId]);

  return (
    <div ref={promptRootRef} className="relative">
      <PromptInput
        className={cn(
          "bg-background/85 rounded-2xl backdrop-blur-sm transition-all duration-300 ease-out *:data-[slot='input-group']:rounded-2xl",
          className,
        )}
        disabled={disabled}
        globalDrop
        multiple
        onSubmit={handleSubmit}
        {...props}
      >
        {extraHeader && (
          <div className="absolute top-0 right-0 left-0 z-10">
            <div className="absolute right-0 bottom-0 left-0 flex items-center justify-center">
              {extraHeader}
            </div>
          </div>
        )}
        {selectedDataSources.length > 0 && (
          <div ref={selectionAreaRef} className="px-4 pt-4 pb-2">
            <div className="flex flex-wrap items-start gap-2">
              {selectedDataSources.map((source) => (
                <div
                  key={source.id}
                  className="flex min-w-0 max-w-[18rem] items-start gap-2 rounded-2xl border bg-white/90 px-3 py-2 shadow-sm"
                >
                  <button
                    type="button"
                    onClick={() => setDataDialogOpen(true)}
                    className="flex min-w-0 items-start gap-2 text-left"
                  >
                    <div className="bg-muted mt-0.5 rounded-xl p-2">
                      <DatabaseIcon className="text-muted-foreground size-3.5" />
                    </div>
                    <div className="min-w-0">
                      <div className="truncate text-sm font-medium">
                        {source.name}
                      </div>
                      <div className="text-muted-foreground mt-0.5 text-[11px]">
                        {source.type === "uploaded_file"
                          ? t.dataCenter.uploadedFile
                          : source.type === "database"
                            ? t.dataCenter.database
                            : source.type === "vector_store"
                              ? t.dataCenter.vectorStore
                              : t.dataCenter.localDataset}
                      </div>
                    </div>
                  </button>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="text-muted-foreground size-6 shrink-0 rounded-full"
                    onClick={() =>
                      setSelectedDataSourceIds((current) =>
                        current.filter((id) => id !== source.id),
                      )
                    }
                  >
                    <XIcon className="size-3.5" />
                  </Button>
                </div>
              ))}
              {selectedDataSources.length > 1 && (
                <Button
                  type="button"
                  variant="ghost"
                  className="text-muted-foreground h-11 rounded-2xl border border-dashed px-3 text-xs"
                  onClick={clearDataSourceSelection}
                >
                  清空全部
                </Button>
              )}
            </div>
          </div>
        )}
        <PromptInputAttachments>
          {(attachment) => <PromptInputAttachment data={attachment} />}
        </PromptInputAttachments>
        <PromptInputBody
          className="absolute right-0 left-0 z-3"
          style={{ top: selectionAreaHeight > 0 ? `${selectionAreaHeight + 8}px` : 0 }}
        >
          <PromptInputTextarea
            className={cn("size-full")}
            disabled={disabled}
            placeholder={t.inputBox.placeholder}
            autoFocus={autoFocus}
            defaultValue={initialValue}
          />
        </PromptInputBody>
        <PromptInputFooter className="flex">
          <PromptInputTools>
            <PromptInputActionMenu>
              <PromptInputActionMenuTrigger className="px-2!" />
              <PromptInputActionMenuContent>
                <PromptInputActionMenuItem onSelect={() => attachments.openFileDialog()}>
                  <PaperclipIcon className="size-4" />
                  {t.inputBox.addAttachments}
                </PromptInputActionMenuItem>
                <PromptInputActionMenuItem onSelect={() => setDataDialogOpen(true)}>
                  <DatabaseIcon className="size-4" />
                  {t.dataCenter.selectForChat}
                </PromptInputActionMenuItem>
              </PromptInputActionMenuContent>
            </PromptInputActionMenu>
            {selectedDataSources.length > 0 && (
              <button
                type="button"
                onClick={() => setDataDialogOpen(true)}
                className="text-muted-foreground inline-flex items-center gap-2 px-2 text-xs"
              >
                <DatabaseIcon className="size-3" />
                <span>
                  {t.dataCenter.selectedDataset} {selectedDataSources.length}
                </span>
              </button>
            )}
            <PromptInputActionMenu>
            <ModeHoverGuide
              mode={
                context.mode === "flash" ||
                  context.mode === "thinking" ||
                  context.mode === "pro" ||
                  context.mode === "ultra"
                  ? context.mode
                  : "flash"
              }
            >
              <PromptInputActionMenuTrigger className="gap-1! px-2!">
                <div>
                  {context.mode === "flash" && <ZapIcon className="size-3" />}
                  {context.mode === "thinking" && (
                    <LightbulbIcon className="size-3" />
                  )}
                  {context.mode === "pro" && (
                    <GraduationCapIcon className="size-3" />
                  )}
                  {context.mode === "ultra" && (
                    <RocketIcon className="size-3 text-[#dabb5e]" />
                  )}
                </div>
                <div
                  className={cn(
                    "text-xs font-normal",
                    context.mode === "ultra" ? "golden-text" : "",
                  )}
                >
                  {(context.mode === "flash" && t.inputBox.flashMode) ||
                    (context.mode === "thinking" && t.inputBox.reasoningMode) ||
                    (context.mode === "pro" && t.inputBox.proMode) ||
                    (context.mode === "ultra" && t.inputBox.ultraMode)}
                </div>
              </PromptInputActionMenuTrigger>
            </ModeHoverGuide>
            <PromptInputActionMenuContent className="w-80">
              <DropdownMenuGroup>
                <DropdownMenuLabel className="text-muted-foreground text-xs">
                  {t.inputBox.mode}
                </DropdownMenuLabel>
                <PromptInputActionMenu>
                  <PromptInputActionMenuItem
                    className={cn(
                      context.mode === "flash"
                        ? "text-accent-foreground"
                        : "text-muted-foreground/65",
                    )}
                    onSelect={() => handleModeSelect("flash")}
                  >
                    <div className="flex flex-col gap-2">
                      <div className="flex items-center gap-1 font-bold">
                        <ZapIcon
                          className={cn(
                            "mr-2 size-4",
                            context.mode === "flash" &&
                            "text-accent-foreground",
                          )}
                        />
                        {t.inputBox.flashMode}
                      </div>
                      <div className="pl-7 text-xs">
                        {t.inputBox.flashModeDescription}
                      </div>
                    </div>
                    {context.mode === "flash" ? (
                      <CheckIcon className="ml-auto size-4" />
                    ) : (
                      <div className="ml-auto size-4" />
                    )}
                  </PromptInputActionMenuItem>
                  {supportThinking && (
                    <PromptInputActionMenuItem
                      className={cn(
                        context.mode === "thinking"
                          ? "text-accent-foreground"
                          : "text-muted-foreground/65",
                      )}
                      onSelect={() => handleModeSelect("thinking")}
                    >
                      <div className="flex flex-col gap-2">
                        <div className="flex items-center gap-1 font-bold">
                          <LightbulbIcon
                            className={cn(
                              "mr-2 size-4",
                              context.mode === "thinking" &&
                              "text-accent-foreground",
                            )}
                          />
                          {t.inputBox.reasoningMode}
                        </div>
                        <div className="pl-7 text-xs">
                          {t.inputBox.reasoningModeDescription}
                        </div>
                      </div>
                      {context.mode === "thinking" ? (
                        <CheckIcon className="ml-auto size-4" />
                      ) : (
                        <div className="ml-auto size-4" />
                      )}
                    </PromptInputActionMenuItem>
                  )}
                  <PromptInputActionMenuItem
                    className={cn(
                      context.mode === "pro"
                        ? "text-accent-foreground"
                        : "text-muted-foreground/65",
                    )}
                    onSelect={() => handleModeSelect("pro")}
                  >
                    <div className="flex flex-col gap-2">
                      <div className="flex items-center gap-1 font-bold">
                        <GraduationCapIcon
                          className={cn(
                            "mr-2 size-4",
                            context.mode === "pro" && "text-accent-foreground",
                          )}
                        />
                        {t.inputBox.proMode}
                      </div>
                      <div className="pl-7 text-xs">
                        {t.inputBox.proModeDescription}
                      </div>
                    </div>
                    {context.mode === "pro" ? (
                      <CheckIcon className="ml-auto size-4" />
                    ) : (
                      <div className="ml-auto size-4" />
                    )}
                  </PromptInputActionMenuItem>
                  <PromptInputActionMenuItem
                    className={cn(
                      context.mode === "ultra"
                        ? "text-accent-foreground"
                        : "text-muted-foreground/65",
                    )}
                    onSelect={() => handleModeSelect("ultra")}
                  >
                    <div className="flex flex-col gap-2">
                      <div className="flex items-center gap-1 font-bold">
                        <RocketIcon
                          className={cn(
                            "mr-2 size-4",
                            context.mode === "ultra" && "text-[#dabb5e]",
                          )}
                        />
                        <div
                          className={cn(
                            context.mode === "ultra" && "golden-text",
                          )}
                        >
                          {t.inputBox.ultraMode}
                        </div>
                      </div>
                      <div className="pl-7 text-xs">
                        {t.inputBox.ultraModeDescription}
                      </div>
                    </div>
                    {context.mode === "ultra" ? (
                      <CheckIcon className="ml-auto size-4" />
                    ) : (
                      <div className="ml-auto size-4" />
                    )}
                  </PromptInputActionMenuItem>
                </PromptInputActionMenu>
              </DropdownMenuGroup>
            </PromptInputActionMenuContent>
            </PromptInputActionMenu>
            {supportReasoningEffort && context.mode !== "flash" && (
              <PromptInputActionMenu>
              <PromptInputActionMenuTrigger className="gap-1! px-2!">
                <div className="text-xs font-normal">
                  {t.inputBox.reasoningEffort}:
                  {context.reasoning_effort === "minimal" && " " + t.inputBox.reasoningEffortMinimal}
                  {context.reasoning_effort === "low" && " " + t.inputBox.reasoningEffortLow}
                  {context.reasoning_effort === "medium" && " " + t.inputBox.reasoningEffortMedium}
                  {context.reasoning_effort === "high" && " " + t.inputBox.reasoningEffortHigh}
                </div>
              </PromptInputActionMenuTrigger>
              <PromptInputActionMenuContent className="w-70">
                <DropdownMenuGroup>
                  <DropdownMenuLabel className="text-muted-foreground text-xs">
                    {t.inputBox.reasoningEffort}
                  </DropdownMenuLabel>
                  <PromptInputActionMenu>
                    <PromptInputActionMenuItem
                      className={cn(
                        context.reasoning_effort === "minimal"
                          ? "text-accent-foreground"
                          : "text-muted-foreground/65",
                      )}
                      onSelect={() => handleReasoningEffortSelect("minimal")}
                    >
                      <div className="flex flex-col gap-2">
                        <div className="flex items-center gap-1 font-bold">
                          {t.inputBox.reasoningEffortMinimal}
                        </div>
                        <div className="pl-2 text-xs">
                          {t.inputBox.reasoningEffortMinimalDescription}
                        </div>
                      </div>
                      {context.reasoning_effort === "minimal" ? (
                        <CheckIcon className="ml-auto size-4" />
                      ) : (
                        <div className="ml-auto size-4" />
                      )}
                    </PromptInputActionMenuItem>
                    <PromptInputActionMenuItem
                      className={cn(
                        context.reasoning_effort === "low"
                          ? "text-accent-foreground"
                          : "text-muted-foreground/65",
                      )}
                      onSelect={() => handleReasoningEffortSelect("low")}
                    >
                      <div className="flex flex-col gap-2">
                        <div className="flex items-center gap-1 font-bold">
                          {t.inputBox.reasoningEffortLow}
                        </div>
                        <div className="pl-2 text-xs">
                          {t.inputBox.reasoningEffortLowDescription}
                        </div>
                      </div>
                      {context.reasoning_effort === "low" ? (
                        <CheckIcon className="ml-auto size-4" />
                      ) : (
                        <div className="ml-auto size-4" />
                      )}
                    </PromptInputActionMenuItem>
                    <PromptInputActionMenuItem
                      className={cn(
                        context.reasoning_effort === "medium" || !context.reasoning_effort
                          ? "text-accent-foreground"
                          : "text-muted-foreground/65",
                      )}
                      onSelect={() => handleReasoningEffortSelect("medium")}
                    >
                      <div className="flex flex-col gap-2">
                        <div className="flex items-center gap-1 font-bold">
                          {t.inputBox.reasoningEffortMedium}
                        </div>
                        <div className="pl-2 text-xs">
                          {t.inputBox.reasoningEffortMediumDescription}
                        </div>
                      </div>
                      {context.reasoning_effort === "medium" || !context.reasoning_effort ? (
                        <CheckIcon className="ml-auto size-4" />
                      ) : (
                        <div className="ml-auto size-4" />
                      )}
                    </PromptInputActionMenuItem>
                    <PromptInputActionMenuItem
                      className={cn(
                        context.reasoning_effort === "high"
                          ? "text-accent-foreground"
                          : "text-muted-foreground/65",
                      )}
                      onSelect={() => handleReasoningEffortSelect("high")}
                    >
                      <div className="flex flex-col gap-2">
                        <div className="flex items-center gap-1 font-bold">
                          {t.inputBox.reasoningEffortHigh}
                        </div>
                        <div className="pl-2 text-xs">
                          {t.inputBox.reasoningEffortHighDescription}
                        </div>
                      </div>
                      {context.reasoning_effort === "high" ? (
                        <CheckIcon className="ml-auto size-4" />
                      ) : (
                        <div className="ml-auto size-4" />
                      )}
                    </PromptInputActionMenuItem>
                  </PromptInputActionMenu>
                </DropdownMenuGroup>
              </PromptInputActionMenuContent>
              </PromptInputActionMenu>
            )}
          </PromptInputTools>
          <PromptInputTools>
            <ModelSelector
              open={modelDialogOpen}
              onOpenChange={setModelDialogOpen}
            >
              <ModelSelectorTrigger asChild>
                <PromptInputButton>
                  <ModelSelectorName className="text-xs font-normal">
                    {selectedModel?.display_name}
                  </ModelSelectorName>
                </PromptInputButton>
              </ModelSelectorTrigger>
              <ModelSelectorContent>
                <ModelSelectorInput placeholder={t.inputBox.searchModels} />
                <ModelSelectorList>
                  {models.map((m) => (
                    <ModelSelectorItem
                      key={m.name}
                      value={m.name}
                      onSelect={() => handleModelSelect(m.name)}
                    >
                      <ModelSelectorName>{m.display_name}</ModelSelectorName>
                      {m.name === context.model_name ? (
                        <CheckIcon className="ml-auto size-4" />
                      ) : (
                        <div className="ml-auto size-4" />
                      )}
                    </ModelSelectorItem>
                  ))}
                </ModelSelectorList>
              </ModelSelectorContent>
            </ModelSelector>
            <PromptInputSubmit
              className="rounded-full"
              disabled={disabled}
              variant="outline"
              status={status}
            />
          </PromptInputTools>
        </PromptInputFooter>
      {isNewThread && searchParams.get("mode") !== "skill" && (
        <div className="absolute right-0 -bottom-20 left-0 z-0 flex items-center justify-center">
          <SuggestionList />
        </div>
      )}
      {!isNewThread && (
        <div className="bg-background absolute right-0 -bottom-[17px] left-0 z-0 h-4"></div>
      )}
      </PromptInput>

      {!disabled &&
        !isNewThread &&
        !followupsHidden &&
        (followupsLoading || followups.length > 0) && (
          <div className="absolute right-0 -top-20 left-0 z-20 flex items-center justify-center">
            <div className="flex items-center gap-2">
              {followupsLoading ? (
                <div className="text-muted-foreground bg-background/80 rounded-full border px-4 py-2 text-xs backdrop-blur-sm">
                  {t.inputBox.followupLoading}
                </div>
              ) : (
                <Suggestions className="min-h-16 w-fit items-start">
                  {followups.map((s) => (
                    <Suggestion
                      key={s}
                      suggestion={s}
                      onClick={() => handleFollowupClick(s)}
                    />
                  ))}
                  <Button
                    aria-label={t.common.close}
                    className="text-muted-foreground cursor-pointer rounded-full px-3 text-xs font-normal"
                    variant="outline"
                    size="sm"
                    type="button"
                    onClick={() => setFollowupsHidden(true)}
                  >
                    <XIcon className="size-4" />
                  </Button>
                </Suggestions>
              )}
            </div>
          </div>
        )}

      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t.inputBox.followupConfirmTitle}</DialogTitle>
            <DialogDescription>
              {t.inputBox.followupConfirmDescription}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmOpen(false)}>
              {t.common.cancel}
            </Button>
            <Button variant="secondary" onClick={confirmAppendAndSend}>
              {t.inputBox.followupConfirmAppend}
            </Button>
            <Button onClick={confirmReplaceAndSend}>
              {t.inputBox.followupConfirmReplace}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={dataDialogOpen} onOpenChange={setDataDialogOpen}>
        <DialogContent className="sm:max-w-4xl">
          <DialogHeader>
            <DialogTitle>{t.dataCenter.availableSources}</DialogTitle>
            <DialogDescription>{t.dataCenter.chatHint}</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="flex flex-wrap items-center gap-3">
              <div className="bg-muted inline-flex rounded-xl p-1">
                <button
                  type="button"
                  onClick={() => setDataDialogTab("sources")}
                  className={cn(
                    "rounded-lg px-4 py-2 text-sm transition",
                    dataDialogTab === "sources"
                      ? "bg-background shadow-sm"
                      : "text-muted-foreground",
                  )}
                >
                  {t.dataCenter.allSources}
                </button>
                <button
                  type="button"
                  onClick={() => setDataDialogTab("uploads")}
                  className={cn(
                    "rounded-lg px-4 py-2 text-sm transition",
                    dataDialogTab === "uploads"
                      ? "bg-background shadow-sm"
                      : "text-muted-foreground",
                  )}
                >
                  {t.dataCenter.uploadedData}
                </button>
              </div>
              <div className="relative min-w-[16rem] flex-1">
                <SearchIcon className="text-muted-foreground absolute top-1/2 left-3 size-4 -translate-y-1/2" />
                <input
                  value={dataDialogQuery}
                  onChange={(event) => setDataDialogQuery(event.target.value)}
                  placeholder={t.dataCenter.searchPlaceholder}
                  className="bg-background h-10 w-full rounded-xl border pl-9 pr-3 text-sm outline-none transition focus:border-neutral-400"
                />
              </div>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setDataDialogOpen(false);
                  router.push("/workspace/data-center");
                }}
              >
                {t.dataCenter.addData}
              </Button>
            </div>

            <div className="overflow-hidden rounded-2xl border">
              <div className="text-muted-foreground grid grid-cols-[56px_minmax(0,1.6fr)_120px_180px] border-b bg-muted/40 px-4 py-3 text-xs font-medium">
                <div />
                <div>{t.dataCenter.availableSources}</div>
                <div>{t.dataCenter.sourceType}</div>
                <div>{t.dataCenter.sourceUpdatedAt}</div>
              </div>
              <div className="max-h-[24rem] overflow-y-auto">
                {dialogSources.length > 0 ? (
                  dialogSources.map((source) => {
                    const selected = draftSelectedDataSourceIds.includes(source.id);
                    return (
                      <button
                        key={source.id}
                        type="button"
                        onClick={() => toggleDraftDataSource(source.id)}
                        className={cn(
                          "grid w-full grid-cols-[56px_minmax(0,1.6fr)_120px_180px] items-center border-b px-4 py-3 text-left transition last:border-b-0",
                          selected ? "bg-primary/5" : "hover:bg-muted/40",
                        )}
                      >
                        <div className="flex justify-center">
                          <span
                            className={cn(
                              "flex size-5 items-center justify-center rounded-md border transition",
                              selected
                                ? "border-primary bg-primary text-primary-foreground"
                                : "bg-background border-neutral-300",
                            )}
                          >
                            {selected && <CheckIcon className="size-3.5" />}
                          </span>
                        </div>
                        <div className="min-w-0">
                          <div className="truncate text-sm font-medium">
                            {source.name}
                          </div>
                          <div className="text-muted-foreground mt-1 truncate text-xs">
                            {source.description || source.path || "-"}
                          </div>
                        </div>
                        <div className="text-muted-foreground text-sm">
                          {source.type === "uploaded_file"
                            ? t.dataCenter.uploadedFile
                            : source.type === "database"
                              ? t.dataCenter.database
                              : source.type === "vector_store"
                                ? t.dataCenter.vectorStore
                                : t.dataCenter.localDataset}
                        </div>
                        <div className="text-muted-foreground text-sm">
                          {source.updated_at
                            ? source.updated_at.slice(0, 10)
                            : "-"}
                        </div>
                      </button>
                    );
                  })
                ) : (
                  <div className="text-muted-foreground px-4 py-10 text-center text-sm">
                    {t.dataCenter.emptyDescription}
                  </div>
                )}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDataDialogOpen(false)}>
              {t.common.cancel}
            </Button>
            <Button onClick={confirmDataSourceSelection}>
              {t.common.save}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function SuggestionList() {
  const { t } = useI18n();
  const { textInput } = usePromptInputController();
  const handleSuggestionClick = useCallback(
    (prompt: string | undefined) => {
      if (!prompt) return;
      textInput.setInput(prompt);
      setTimeout(() => {
        const textarea = document.querySelector<HTMLTextAreaElement>(
          "textarea[name='message']",
        );
        if (textarea) {
          const selStart = prompt.indexOf("[");
          const selEnd = prompt.indexOf("]");
          if (selStart !== -1 && selEnd !== -1) {
            textarea.setSelectionRange(selStart, selEnd + 1);
            textarea.focus();
          }
        }
      }, 500);
    },
    [textInput],
  );
  return (
    <Suggestions className="min-h-16 w-fit items-start">
      <ConfettiButton
        className="text-muted-foreground cursor-pointer rounded-full px-4 text-xs font-normal"
        variant="outline"
        size="sm"
        onClick={() => handleSuggestionClick(t.inputBox.surpriseMePrompt)}
      >
        <SparklesIcon className="size-4" /> {t.inputBox.surpriseMe}
      </ConfettiButton>
      {t.inputBox.suggestions.map((suggestion) => (
        <Suggestion
          key={suggestion.suggestion}
          icon={suggestion.icon}
          suggestion={suggestion.suggestion}
          onClick={() => handleSuggestionClick(suggestion.prompt)}
        />
      ))}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Suggestion icon={PlusIcon} suggestion={t.common.create} />
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          <DropdownMenuGroup>
            {t.inputBox.suggestionsCreate.map((suggestion, index) =>
              "type" in suggestion && suggestion.type === "separator" ? (
                <DropdownMenuSeparator key={index} />
              ) : (
                !("type" in suggestion) && (
                  <DropdownMenuItem
                    key={suggestion.suggestion}
                    onClick={() => handleSuggestionClick(suggestion.prompt)}
                  >
                    {suggestion.icon && <suggestion.icon className="size-4" />}
                    {suggestion.suggestion}
                  </DropdownMenuItem>
                )
              ),
            )}
          </DropdownMenuGroup>
        </DropdownMenuContent>
      </DropdownMenu>
    </Suggestions>
  );
}
