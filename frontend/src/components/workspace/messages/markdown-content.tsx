"use client";

import { useMemo } from "react";
import type { HTMLAttributes } from "react";
import { useParams } from "next/navigation";

import {
  MessageResponse,
  type MessageResponseProps,
} from "@/components/ai-elements/message";
import { resolveArtifactURL } from "@/core/artifacts/utils";
import { streamdownPlugins } from "@/core/streamdown";

import { CitationLink } from "../citations/citation-link";

export type MarkdownContentProps = {
  content: string;
  isLoading: boolean;
  rehypePlugins: MessageResponseProps["rehypePlugins"];
  className?: string;
  remarkPlugins?: MessageResponseProps["remarkPlugins"];
  components?: MessageResponseProps["components"];
};

/** Renders markdown content. */
export function MarkdownContent({
  content,
  rehypePlugins,
  className,
  remarkPlugins = streamdownPlugins.remarkPlugins,
  components: componentsFromProps,
}: MarkdownContentProps) {
  const { thread_id } = useParams<{ thread_id: string }>();

  const resolveHref = (href: string): string => {
    const raw = String(href || "").trim();
    if (!raw) return raw;
    if (raw.startsWith("#")) return raw;
    if (/^(https?:|mailto:|tel:)/i.test(raw)) return raw;
    if (raw.startsWith("/api/")) return raw;

    // `/mnt/user-data/...` virtual path -> thread artifact URL
    if (raw.startsWith("/mnt/")) {
      return thread_id ? resolveArtifactURL(raw, thread_id) : raw;
    }

    // 兼容报告中常见的相对文件链接（如 `traffic_map_xxx.html`）
    // 将其映射到当前线程 outputs artifacts，避免被前端当成聊天路由跳转。
    const normalized = raw.replace(/^\.\/+/, "");
    const isLikelyOutputFile =
      !normalized.startsWith("/") &&
      !normalized.startsWith("../") &&
      /\.(html?|geojson|json|md|txt|csv|tsv|png|jpe?g|webp|gif|svg|pdf)$/i.test(normalized);
    if (isLikelyOutputFile && thread_id) {
      return resolveArtifactURL(`/mnt/user-data/outputs/${normalized}`, thread_id);
    }
    return raw;
  };

  const components = useMemo(() => {
    return {
      a: (props: HTMLAttributes<HTMLAnchorElement>) => {
        const href = String((props as { href?: string }).href || "");
        const finalHref = resolveHref(href);
        if (typeof props.children === "string") {
          const match = /^citation:(.+)$/.exec(props.children);
          if (match) {
            const [, text] = match;
            return (
              <CitationLink {...props} href={finalHref} target="_blank" rel="noopener noreferrer">
                {text}
              </CitationLink>
            );
          }
        }
        return <a {...props} href={finalHref} target="_blank" rel="noopener noreferrer" />;
      },
      ...componentsFromProps,
    };
  }, [componentsFromProps, thread_id]);

  if (!content) return null;

  return (
    <MessageResponse
      className={className}
      remarkPlugins={remarkPlugins}
      rehypePlugins={rehypePlugins}
      components={components}
    >
      {content}
    </MessageResponse>
  );
}
