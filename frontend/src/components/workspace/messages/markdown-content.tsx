"use client";

import { useMemo } from "react";
import type { HTMLAttributes } from "react";

import {
  MessageResponse,
  type MessageResponseProps,
} from "@/components/ai-elements/message";
import { streamdownPlugins } from "@/core/streamdown";

import { CitationLink } from "../citations/citation-link";

const HOST_FILE_ROUTE_PREFIX = "/api/host-files";
const HOST_FILE_PATH_PREFIXES = ["/mnt/nas", "/home/anker/imiss-deer-flow/datasets"];


function resolveHostFileHref(href?: string) {
  if (!href) {
    return href;
  }

  if (href.startsWith(HOST_FILE_ROUTE_PREFIX)) {
    return href;
  }

  const matchedPrefix = HOST_FILE_PATH_PREFIXES.find(
    (prefix) => href === prefix || href.startsWith(`${prefix}/`),
  );

  if (!matchedPrefix) {
    return href;
  }

  return `${HOST_FILE_ROUTE_PREFIX}${href}`;
}

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
  const components = useMemo(() => {
    return {
      a: (props: HTMLAttributes<HTMLAnchorElement>) => {
        if (typeof props.children === "string") {
          const match = /^citation:(.+)$/.exec(props.children);
          if (match) {
            const [, text] = match;
            return <CitationLink {...props}>{text}</CitationLink>;
          }
        }

        const href = resolveHostFileHref(props.href);
        const shouldOpenInNewTab = href !== props.href && typeof href === "string";

        return <a {...props} href={href} target={shouldOpenInNewTab ? "_blank" : props.target} rel={shouldOpenInNewTab ? "noopener noreferrer" : props.rel} />;
      },
      ...componentsFromProps,
    };
  }, [componentsFromProps]);

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
