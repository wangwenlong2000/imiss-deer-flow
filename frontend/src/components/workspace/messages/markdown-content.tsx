"use client";

import { useMemo } from "react";
import type { AnchorHTMLAttributes } from "react";

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
  threadId?: string;
};

/** Renders markdown content. */
export function MarkdownContent({
  content,
  rehypePlugins,
  className,
  remarkPlugins = streamdownPlugins.remarkPlugins,
  components: componentsFromProps,
  threadId,
}: MarkdownContentProps) {
  const components = useMemo(() => {
    return {
      a: (props: AnchorHTMLAttributes<HTMLAnchorElement>) => {
        const href = threadId && props.href?.startsWith("/mnt/user-data/")
          ? resolveArtifactURL(props.href, threadId)
          : props.href;
        if (typeof props.children === "string") {
          const match = /^citation:(.+)$/.exec(props.children);
          if (match) {
            const [, text] = match;
            return <CitationLink {...props}>{text}</CitationLink>;
          }
        }
        return <a {...props} href={href} />;
      },
      ...componentsFromProps,
    };
  }, [componentsFromProps, threadId]);

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
