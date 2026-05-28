import type { Message } from "@langchain/langgraph-sdk";
import {
  BookOpenTextIcon,
  FolderOpenIcon,
  GlobeIcon,
  LightbulbIcon,
  ListTodoIcon,
  MessageCircleQuestionMarkIcon,
  NotebookPenIcon,
  SearchIcon,
  SquareTerminalIcon,
  WrenchIcon,
} from "lucide-react";
import { useMemo, type ReactNode } from "react";

import {
  ChainOfThought,
  ChainOfThoughtHeader,
  ChainOfThoughtContent,
  ChainOfThoughtSearchResult,
  ChainOfThoughtSearchResults,
  ChainOfThoughtStep,
} from "@/components/ai-elements/chain-of-thought";
import { CodeBlock } from "@/components/ai-elements/code-block";
import { useI18n } from "@/core/i18n/hooks";
import {
  extractTextFromMessage,
  extractReasoningContentFromMessage,
  findToolCallResult,
  isHiddenStepMessage,
} from "@/core/messages/utils";
import { useRehypeSplitWordsIntoSpans } from "@/core/rehype";
import { extractTitleFromMarkdown } from "@/core/utils/markdown";
import { cn } from "@/lib/utils";

import { useArtifacts } from "../artifacts";
import { FlipDisplay } from "../flip-display";
import { Tooltip } from "../tooltip";

import { MarkdownContent } from "./markdown-content";

export function MessageGroup({
  className,
  messages,
  isLoading = false,
}: {
  className?: string;
  messages: Message[];
  isLoading?: boolean;
}) {
  const steps = useMemo(() => convertToSteps(messages), [messages]);
  const hiddenSteps = useMemo(
    () => steps.filter((step) => step.type === "hiddenStep"),
    [steps],
  );
  const lastToolCallStep = useMemo(() => {
    const filteredSteps = steps.filter((step) => step.type === "toolCall");
    return filteredSteps[filteredSteps.length - 1];
  }, [steps]);
  const aboveLastToolCallSteps = useMemo(() => {
    if (lastToolCallStep) {
      const index = steps.indexOf(lastToolCallStep);
      return steps
        .slice(0, index)
        .filter((step) => step.type !== "hiddenStep");
    }
    return [];
  }, [lastToolCallStep, steps]);
  const lastReasoningStep = useMemo(() => {
    if (lastToolCallStep) {
      const index = steps.indexOf(lastToolCallStep);
      return steps.slice(index + 1).find((step) => step.type === "reasoning");
    } else {
      const filteredSteps = steps.filter((step) => step.type === "reasoning");
      return filteredSteps[filteredSteps.length - 1];
    }
  }, [lastToolCallStep, steps]);
  const rehypePlugins = useRehypeSplitWordsIntoSpans(isLoading);
  const hasAnyHiddenContent =
    hiddenSteps.length > 0 ||
    aboveLastToolCallSteps.length > 0 ||
    Boolean(lastToolCallStep) ||
    Boolean(lastReasoningStep);
  if (!hasAnyHiddenContent) {
    return null;
  }

  return (
    <ChainOfThought
      className={cn("w-full gap-2 rounded-lg border p-0.5", className)}
      defaultOpen={false}
    >
      <ChainOfThoughtHeader className="px-4 py-2" icon={<LightbulbIcon className="size-4" />}>
        隐藏步骤
      </ChainOfThoughtHeader>
      <ChainOfThoughtContent className="px-4 pb-3">
        {hiddenSteps.map((step) => (
          <ChainOfThoughtStep
            key={step.id}
            icon={LightbulbIcon}
            label={
              <HiddenStepLabel
                step={step}
                isLoading={isLoading}
                rehypePlugins={rehypePlugins}
              />
            }
          />
        ))}
        {aboveLastToolCallSteps.map((step) =>
          step.type === "reasoning" ? (
            <ChainOfThoughtStep
              key={step.id}
              icon={LightbulbIcon}
              label={
                <MarkdownContent
                  content={step.reasoning ?? ""}
                  isLoading={isLoading}
                  rehypePlugins={rehypePlugins}
                />
              }
            />
          ) : (
            <ToolCall key={step.id} {...step} isLoading={isLoading} />
          ),
        )}
        {lastToolCallStep && (
          <FlipDisplay uniqueKey={lastToolCallStep.id ?? ""}>
            <ToolCall
              key={lastToolCallStep.id}
              {...lastToolCallStep}
              isLast={true}
              isLoading={isLoading}
            />
          </FlipDisplay>
        )}
        {lastReasoningStep && (
          <ChainOfThoughtStep
            key={lastReasoningStep.id}
            icon={LightbulbIcon}
            label={
              <MarkdownContent
                content={lastReasoningStep.reasoning ?? ""}
                isLoading={isLoading}
                rehypePlugins={rehypePlugins}
              />
            }
          />
        )}
      </ChainOfThoughtContent>
    </ChainOfThought>
  );
}

function ToolCall({
  id,
  messageId,
  name,
  args,
  result,
  isLast = false,
  isLoading = false,
}: {
  id?: string;
  messageId?: string;
  name: string;
  args: Record<string, unknown>;
  result?: string | Record<string, unknown>;
  isLast?: boolean;
  isLoading?: boolean;
}) {
  const { t } = useI18n();
  const { setOpen, autoOpen, autoSelect, selectedArtifact, select } =
    useArtifacts();

  if (name === "web_search") {
    let label: React.ReactNode = t.toolCalls.searchForRelatedInfo;
    if (typeof args.query === "string") {
      label = t.toolCalls.searchOnWebFor(args.query);
    }
    return (
      <ChainOfThoughtStep key={id} label={label} icon={SearchIcon}>
        {Array.isArray(result) && (
          <ChainOfThoughtSearchResults>
            {result.map((item) => (
              <ChainOfThoughtSearchResult key={item.url}>
                <a href={item.url} target="_blank" rel="noreferrer">
                  {item.title}
                </a>
              </ChainOfThoughtSearchResult>
            ))}
          </ChainOfThoughtSearchResults>
        )}
      </ChainOfThoughtStep>
    );
  } else if (name === "image_search") {
    let label: React.ReactNode = t.toolCalls.searchForRelatedImages;
    if (typeof args.query === "string") {
      label = t.toolCalls.searchForRelatedImagesFor(args.query);
    }
    const results = (
      result as {
        results: {
          source_url: string;
          thumbnail_url: string;
          image_url: string;
          title: string;
        }[];
      }
    )?.results;
    return (
      <ChainOfThoughtStep key={id} label={label} icon={SearchIcon}>
        {Array.isArray(results) && (
          <ChainOfThoughtSearchResults>
            {Array.isArray(results) &&
              results.map((item) => (
                <Tooltip key={item.image_url} content={item.title}>
                  <a
                    className="size-24 overflow-hidden rounded-lg object-cover"
                    href={item.source_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    <div className="bg-accent size-24">
                      <img
                        className="size-full object-cover"
                        src={item.thumbnail_url}
                        alt={item.title}
                        width={100}
                        height={100}
                      />
                    </div>
                  </a>
                </Tooltip>
              ))}
          </ChainOfThoughtSearchResults>
        )}
      </ChainOfThoughtStep>
    );
  } else if (name === "web_fetch") {
    const url = (args as { url: string })?.url;
    let title = url;
    if (typeof result === "string") {
      const potentialTitle = extractTitleFromMarkdown(result);
      if (potentialTitle && potentialTitle.toLowerCase() !== "untitled") {
        title = potentialTitle;
      }
    }
    return (
      <ChainOfThoughtStep
        key={id}
        className="cursor-pointer"
        label={t.toolCalls.viewWebPage}
        icon={GlobeIcon}
        onClick={() => {
          window.open(url, "_blank");
        }}
      >
        <ChainOfThoughtSearchResult>
          {url && (
            <a href={url} target="_blank" rel="noreferrer">
              {title}
            </a>
          )}
        </ChainOfThoughtSearchResult>
      </ChainOfThoughtStep>
    );
  } else if (name === "ls") {
    let description: string | undefined = (args as { description: string })
      ?.description;
    if (!description) {
      description = t.toolCalls.listFolder;
    }
    const path: string | undefined = (args as { path: string })?.path;
    return (
      <ChainOfThoughtStep key={id} label={description} icon={FolderOpenIcon}>
        {path && (
          <ChainOfThoughtSearchResult className="cursor-pointer">
            {path}
          </ChainOfThoughtSearchResult>
        )}
      </ChainOfThoughtStep>
    );
  } else if (name === "read_file") {
    let description: string | undefined = (args as { description: string })
      ?.description;
    if (!description) {
      description = t.toolCalls.readFile;
    }
    const { path } = args as { path: string; content: string };
    return (
      <ChainOfThoughtStep key={id} label={description} icon={BookOpenTextIcon}>
        {path && (
          <ChainOfThoughtSearchResult className="cursor-pointer">
            {path}
          </ChainOfThoughtSearchResult>
        )}
      </ChainOfThoughtStep>
    );
  } else if (name === "write_file" || name === "str_replace") {
    let description: string | undefined = (args as { description: string })
      ?.description;
    if (!description) {
      description = t.toolCalls.writeFile;
    }
    const path: string | undefined = (args as { path: string })?.path;
    if (isLoading && isLast && autoOpen && autoSelect && path) {
      setTimeout(() => {
        const url = new URL(
          `write-file:${path}?message_id=${messageId}&tool_call_id=${id}`,
        ).toString();
        if (selectedArtifact === url) {
          return;
        }
        select(url, true);
        setOpen(true);
      }, 100);
    }

    return (
      <ChainOfThoughtStep
        key={id}
        className="cursor-pointer"
        label={description}
        icon={NotebookPenIcon}
        onClick={() => {
          select(
            new URL(
              `write-file:${path}?message_id=${messageId}&tool_call_id=${id}`,
            ).toString(),
          );
          setOpen(true);
        }}
      >
        {path && (
          <ChainOfThoughtSearchResult className="cursor-pointer">
            {path}
          </ChainOfThoughtSearchResult>
        )}
      </ChainOfThoughtStep>
    );
  } else if (name === "bash") {
    const description: string | undefined = (args as { description: string })
      ?.description;
    if (!description) {
      return t.toolCalls.executeCommand;
    }
    const command: string | undefined = (args as { command: string })?.command;
    return (
      <ChainOfThoughtStep
        key={id}
        label={description}
        icon={SquareTerminalIcon}
      >
        {command && (
          <CodeBlock
            className="mx-0 cursor-pointer border-none px-0"
            showLineNumbers={false}
            language="bash"
            code={command}
          />
        )}
      </ChainOfThoughtStep>
    );
  } else if (name === "ask_clarification") {
    return (
      <ChainOfThoughtStep
        key={id}
        label={t.toolCalls.needYourHelp}
        icon={MessageCircleQuestionMarkIcon}
      ></ChainOfThoughtStep>
    );
  } else if (name === "write_todos") {
    return (
      <ChainOfThoughtStep
        key={id}
        label={t.toolCalls.writeTodos}
        icon={ListTodoIcon}
      ></ChainOfThoughtStep>
    );
  } else {
    const description: string | undefined = (args as { description: string })
      ?.description;
    return (
      <ChainOfThoughtStep
        key={id}
        label={description ?? t.toolCalls.useTool(name)}
        icon={WrenchIcon}
      ></ChainOfThoughtStep>
    );
  }
}

interface GenericCoTStep<T extends string = string> {
  id?: string;
  messageId?: string;
  type: T;
}

interface CoTReasoningStep extends GenericCoTStep<"reasoning"> {
  reasoning: string | null;
}

interface CoTToolCallStep extends GenericCoTStep<"toolCall"> {
  name: string;
  args: Record<string, unknown>;
  result?: string;
}

interface CoTHiddenStep extends GenericCoTStep<"hiddenStep"> {
  title?: string;
  source?: string;
  content: string;
}

type CoTStep = CoTReasoningStep | CoTToolCallStep | CoTHiddenStep;

function convertToSteps(messages: Message[]): CoTStep[] {
  const steps: CoTStep[] = [];
  for (const message of messages) {
    if (isHiddenStepMessage(message)) {
      const hiddenSteps = formatHiddenSteps(extractTextFromMessage(message));
      steps.push(
        ...hiddenSteps.map((step, index) => ({
          id: `${message.id ?? "hidden-step"}-${index}`,
          messageId: message.id,
          type: "hiddenStep" as const,
          ...step,
        })),
      );
      continue;
    }

    if (message.type === "ai") {
      const reasoning = extractReasoningContentFromMessage(message);
      if (reasoning) {
        const step: CoTReasoningStep = {
          id: message.id,
          messageId: message.id,
          type: "reasoning",
          reasoning: extractReasoningContentFromMessage(message),
        };
        steps.push(step);
      }
      for (const tool_call of message.tool_calls ?? []) {
        if (tool_call.name === "task") {
          continue;
        }
        const step: CoTToolCallStep = {
          id: tool_call.id,
          messageId: message.id,
          type: "toolCall",
          name: tool_call.name,
          args: tool_call.args,
        };
        const toolCallId = tool_call.id;
        if (toolCallId) {
          const toolCallResult = findToolCallResult(toolCallId, messages);
          if (toolCallResult) {
            try {
              const json = JSON.parse(toolCallResult);
              step.result = json;
            } catch {
              step.result = toolCallResult;
            }
          }
        }
        steps.push(step);
      }
    }
  }
  return steps;
}

function formatHiddenSteps(
  content: string,
): Array<{ title?: string; source?: string; content: string }> {
  const stripped = content
    .replace(/<\/?system_reminder>/g, "")
    .replace(
      /Use the following current-turn intent and routing guidance when deciding whether to create or update todos\.\s*Treat it as planning context, not as a user request\./g,
      "",
    )
    .trim();

  const matches = Array.from(
    stripped.matchAll(/<hidden_step\b([^>]*)>([\s\S]*?)<\/hidden_step>/g),
  );

  if (matches.length === 0) {
    return stripped ? [{ content: stripped }] : [];
  }

  return matches
    .map((match) => {
      const attrs = match[1] ?? "";
      const body = (match[2] ?? "").trim();
      return {
        title: extractHiddenStepTitle(attrs) ?? undefined,
        source: extractHiddenStepSource(attrs) ?? undefined,
        content: body,
      };
    })
    .filter((step) => step.content.length > 0);
}

function extractHiddenStepTitle(attrs: string) {
  const titleMatch = /\btitle=(["'])(.*?)\1/.exec(attrs);
  if (titleMatch?.[2]) {
    return titleMatch[2];
  }

  const sourceMatch = /\bsource=(["'])(.*?)\1/.exec(attrs);
  const source = sourceMatch?.[2];
  if (source === "intent_recognition") {
    return "意图识别";
  }
  if (source === "skill_router") {
    return "SkillRouter 路由";
  }
  return null;
}

function extractHiddenStepSource(attrs: string) {
  const sourceMatch = /\bsource=(["'])(.*?)\1/.exec(attrs);
  return sourceMatch?.[2] ?? null;
}

function HiddenStepLabel({
  step,
  isLoading,
  rehypePlugins,
}: {
  step: CoTHiddenStep;
  isLoading: boolean;
  rehypePlugins: ReturnType<typeof useRehypeSplitWordsIntoSpans>;
}) {
  const summary =
    step.source === "intent_recognition" || step.title === "意图识别"
      ? parseIntentRecognitionSummary(step.content)
      : step.source === "skill_router" || step.title === "SkillRouter 路由"
      ? parseSkillRouterSummary(step.content)
      : null;

  if (!summary) {
    return (
      <div className="space-y-1">
        {step.title && (
          <div className="text-muted-foreground text-xs font-medium">
            {step.title}
          </div>
        )}
        <MarkdownContent
          content={step.content}
          isLoading={isLoading}
          rehypePlugins={rehypePlugins}
          className="my-0 text-sm"
        />
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="text-muted-foreground text-xs font-medium">
        {step.title ?? "隐藏步骤"}
      </div>
      {summary.routingQuery && (
        <SummaryRow
          label="路由任务"
          value={summary.routingQuery}
        />
      )}
      {summary.sceneLine && (
        <SummaryRow label="识别场景" value={summary.sceneLine} />
      )}
      {summary.routeReason && (
        <SummaryRow label="路由原因" value={summary.routeReason} />
      )}
      {summary.primaryGoal && (
        <SummaryRow label="主要目标" value={summary.primaryGoal} />
      )}
      {summary.taskSpans.length > 0 && (
        <SummaryBlock
          label={summary.taskSpans.length > 1 ? "任务切分" : "任务内容"}
        >
          <ul className="list-disc space-y-1 pl-5">
            {summary.taskSpans.map((text, index) => (
              <li key={`${index}-${text}`}>{text}</li>
            ))}
          </ul>
        </SummaryBlock>
      )}
      {summary.sceneTasks.length > 0 && (
        <SummaryBlock label="场景任务">
          <ul className="space-y-2">
            {summary.sceneTasks.map((task, index) => (
              <li
                key={`${index}-${task.scene}-${task.text}`}
                className="rounded-md border bg-background px-2 py-1"
              >
                <div className="font-medium">
                  {task.scene}
                  {task.sceneName ? `（${task.sceneName}）` : ""}
                </div>
                <div className="text-sm">{task.text}</div>
                {task.params && (
                  <div className="text-muted-foreground text-xs">
                    参数：{task.params}
                  </div>
                )}
              </li>
            ))}
          </ul>
        </SummaryBlock>
      )}
      {summary.routeTasks.length > 0 && (
        <SummaryBlock label="路由分段">
          <ul className="space-y-2">
            {summary.routeTasks.map((task, index) => (
              <li
                key={`${index}-${task.scene}-${task.text}`}
                className="rounded-md border bg-background px-2 py-1"
              >
                <div className="font-medium">
                  {task.text}
                  {task.scene ? `（${task.scene}）` : ""}
                </div>
                {task.skills.length > 0 && (
                  <div className="text-muted-foreground text-xs">
                    推荐 skill：{task.skills.join(", ")}
                  </div>
                )}
              </li>
            ))}
          </ul>
        </SummaryBlock>
      )}
      {summary.taskHints.length > 0 && (
        <SummaryBlock label="任务提示">
          <ul className="list-disc space-y-1 pl-5">
            {summary.taskHints.map((text, index) => (
              <li key={`${index}-${text}`}>{text}</li>
            ))}
          </ul>
        </SummaryBlock>
      )}
      {summary.selectedSkills.length > 0 && (
        <SummaryBlock label="已选技能">
          <div className="flex flex-wrap gap-2">
            {summary.selectedSkills.map((skill, index) => (
              <span
                key={`${index}-${skill}`}
                className="rounded-md bg-muted px-2 py-1 text-xs"
              >
                {skill}
              </span>
            ))}
          </div>
        </SummaryBlock>
      )}
    </div>
  );
}

function SummaryRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="space-y-0.5">
      <div className="text-muted-foreground text-xs">{label}</div>
      <div className="text-sm">{value}</div>
    </div>
  );
}

function SummaryBlock({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-1">
      <div className="text-muted-foreground text-xs">{label}</div>
      {children}
    </div>
  );
}

function parseIntentRecognitionSummary(content: string) {
  return parseRoutingSummary(content);
}

function parseSkillRouterSummary(content: string) {
  return parseRoutingSummary(content);
}

function parseRoutingSummary(content: string) {
  const stripped = content
    .replace(/<\/?system_reminder>/g, "")
    .replace(
      /Use the following current-turn intent and routing guidance when deciding whether to create or update todos\.\s*Treat it as planning context, not as a user request\./g,
      "",
    )
    .trim();

  if (!stripped) {
    return null;
  }

  const summary = {
    routingQuery: "",
    sceneLine: "",
    routeReason: "",
    primaryGoal: "",
    isMultiScene: false,
    taskSpans: [] as string[],
    sceneTasks: [] as Array<{
      scene: string;
      sceneName?: string;
      text: string;
      params?: string;
    }>,
    routeTasks: [] as Array<{
      scene: string;
      text: string;
      skills: string[];
    }>,
    taskHints: [] as string[],
    selectedSkills: [] as string[],
  };

  let currentSection: "task_spans" | "scene_tasks" | "task_hints" | "skills" | null = null;

  for (const rawLine of stripped.split("\n")) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }
    if (line.startsWith("改写后的任务：")) {
      summary.routingQuery = line.replace(/^改写后的任务：/, "").trim();
      currentSection = null;
      continue;
    }
    if (line.startsWith("识别场景：")) {
      summary.sceneLine = line.replace(/^识别场景：/, "").trim();
      summary.isMultiScene = summary.sceneLine.includes("场景模式：multi");
      currentSection = null;
      continue;
    }
    if (line.startsWith("路由原因：")) {
      summary.routeReason = line.replace(/^路由原因：/, "").trim();
      currentSection = null;
      continue;
    }
    if (line.startsWith("主要目标：")) {
      summary.primaryGoal = line.replace(/^主要目标：/, "").trim();
      currentSection = null;
      continue;
    }
    if (line.startsWith("任务切分：")) {
      currentSection = "task_spans";
      continue;
    }
    if (line.startsWith("场景任务：")) {
      currentSection = "scene_tasks";
      continue;
    }
    if (line.startsWith("任务提示：")) {
      currentSection = "task_hints";
      continue;
    }
    if (line.startsWith("本轮可用 skills：")) {
      const skills = line.replace(/^本轮可用 skills：/, "").trim();
      summary.selectedSkills = skills
        .split(",")
        .map((skill) => skill.trim())
        .filter(Boolean);
      currentSection = "skills";
      continue;
    }

    if (!line.startsWith("- ")) {
      continue;
    }

    const value = line.slice(2).trim();
    if (currentSection === "task_spans") {
      summary.taskSpans.push(value);
      continue;
    }
    if (currentSection === "task_hints") {
      summary.taskHints.push(value);
      continue;
    }
    if (currentSection === "skills") {
      summary.selectedSkills.push(value);
      continue;
    }
    if (currentSection === "scene_tasks") {
      const match = /^([^:：]+)(?:（([^）]+)）)?[:：]\s*(.*?)(?:；参数：(.+))?$/.exec(value);
      if (match) {
        summary.sceneTasks.push({
          scene: match[1]?.trim() ?? "",
          sceneName: match[2]?.trim() ?? undefined,
          text: match[3]?.trim() ?? "",
          params: match[4]?.trim() ?? undefined,
        });
      } else {
        summary.sceneTasks.push({ scene: value, text: "" });
      }
      continue;
    }

    if (line.startsWith("- ")) {
      const match = /^-\s*(.*?)(?:；场景：([^；]+))?(?:；(?:使用|推荐) skill：(.+))?$/.exec(line);
      if (match) {
        const scene = match[2]?.trim() ?? "";
        const skills = (match[3] ?? "")
          .split(",")
          .map((skill) => skill.trim())
          .filter(Boolean);
        if (!scene && skills.length === 0) {
          continue;
        }
        summary.routeTasks.push({
          text: match[1]?.trim() ?? "",
          scene,
          skills,
        });
      }
    }
  }

  return summary;
}
