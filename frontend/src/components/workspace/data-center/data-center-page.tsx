"use client";

import {
  CirclePlusIcon,
  DatabaseIcon,
  FolderArchiveIcon,
  HardDriveDownloadIcon,
  RefreshCwIcon,
  SearchIcon,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  readSelectedDataSourceIds,
  uploadDataSourceFiles,
  useDataSourceDetail,
  useDataSources,
  type DataSourceRecord,
  writeSelectedDataSourceIds,
} from "@/core/data-center";
import { useI18n } from "@/core/i18n/hooks";
import { cn } from "@/lib/utils";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";

function labelOfType(
  type: DataSourceRecord["type"],
  t: ReturnType<typeof useI18n>["t"],
) {
  if (type === "local_dataset") return t.dataCenter.localDataset;
  if (type === "uploaded_file") return t.dataCenter.uploadedFile;
  if (type === "database") return t.dataCenter.database;
  return t.dataCenter.vectorStore;
}

function labelOfStatus(
  status: DataSourceRecord["status"],
  t: ReturnType<typeof useI18n>["t"],
) {
  if (status === "ready") return t.dataCenter.ready;
  if (status === "syncing") return t.dataCenter.syncing;
  if (status === "error") return t.dataCenter.error;
  return t.dataCenter.disabled;
}

function iconOfSource(source: DataSourceRecord) {
  if (source.type === "uploaded_file") return HardDriveDownloadIcon;
  if (source.type === "database") return DatabaseIcon;
  return FolderArchiveIcon;
}

export function DataCenterPage() {
  const { t } = useI18n();
  const { data, isLoading, error, refetch } = useDataSources();
  const [query, setQuery] = useState("");
  const [activeTab, setActiveTab] = useState<"sources" | "uploads">("sources");
  const [selectedId, setSelectedId] = useState<string>("");
  const [chatSelection, setChatSelection] = useState<string[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [databaseDialogOpen, setDatabaseDialogOpen] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    document.title = `${t.dataCenter.title} - ${t.pages.appName}`;
  }, [t.dataCenter.title, t.pages.appName]);

  useEffect(() => {
    setChatSelection(readSelectedDataSourceIds());
  }, []);

  const sources = data?.sources ?? [];

  const visibleSources = useMemo(() => {
    return sources
      .filter((source) =>
        activeTab === "uploads"
          ? source.type === "uploaded_file"
          : source.type !== "uploaded_file",
      )
      .filter((source) => {
        const haystack = `${source.name} ${source.description} ${source.path ?? ""}`;
        return haystack.toLowerCase().includes(query.toLowerCase());
      });
  }, [activeTab, query, sources]);

  useEffect(() => {
    if (
      (!selectedId || !visibleSources.some((source) => source.id === selectedId)) &&
      visibleSources[0]?.id
    ) {
      setSelectedId(visibleSources[0].id);
    }
  }, [selectedId, visibleSources]);

  const selectedSource = useMemo(() => {
    return (
      visibleSources.find((source) => source.id === selectedId) ??
      visibleSources[0] ??
      null
    );
  }, [selectedId, visibleSources]);

  const { data: selectedSourceDetail } = useDataSourceDetail(selectedSource?.id);

  const selectedChatSources = useMemo(() => {
    return sources.filter((source) => chatSelection.includes(source.id));
  }, [chatSelection, sources]);

  const handleUseForChat = () => {
    if (!selectedSource) {
      return;
    }

    const nextSelection = chatSelection.includes(selectedSource.id)
      ? chatSelection.filter((id) => id !== selectedSource.id)
      : Array.from(new Set([...chatSelection, selectedSource.id]));

    setChatSelection(nextSelection);
    writeSelectedDataSourceIds(nextSelection);
    if (!chatSelection.includes(selectedSource.id)) {
      toast.success(t.dataCenter.useForChatSuccess);
    }
  };

  const handleSelectFiles = async (event: ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files ?? []);
    if (files.length === 0) {
      return;
    }

    try {
      setIsUploading(true);
      const response = await uploadDataSourceFiles(files);
      await refetch();
      setActiveTab("uploads");
      if (response.sources[0]?.id) {
        setSelectedId(response.sources[0].id);
      }
      toast.success(response.message);
    } catch (error) {
      toast.error(
        error instanceof Error ? error.message : "Failed to upload data sources",
      );
    } finally {
      setIsUploading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  };

  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody className="bg-muted/20">
        <input
          ref={fileInputRef}
          type="file"
          className="hidden"
          multiple
          onChange={(event) => void handleSelectFiles(event)}
        />
        <div className="flex size-full gap-0 overflow-hidden rounded-none xl:p-4">
          <section className="bg-background flex h-full w-full min-w-0 flex-col overflow-hidden border xl:rounded-3xl">
            <div className="grid min-h-0 flex-1 grid-cols-1 xl:grid-cols-[360px_minmax(0,1fr)]">
              <aside className="border-r">
                <div className="flex h-20 items-center justify-between border-b px-6">
                  <div>
                    <h1 className="text-lg font-semibold">{t.dataCenter.title}</h1>
                    <p className="text-muted-foreground mt-1 text-sm">
                      {t.dataCenter.subtitle}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      size="icon"
                      variant="ghost"
                      aria-label={t.dataCenter.refresh}
                      onClick={() => void refetch()}
                    >
                      <RefreshCwIcon className="size-4" />
                    </Button>
                    <Button
                      size="icon"
                      variant="ghost"
                      aria-label={t.dataCenter.addData}
                      onClick={() => fileInputRef.current?.click()}
                      disabled={isUploading}
                    >
                      <CirclePlusIcon className="size-4" />
                    </Button>
                  </div>
                </div>

                <div className="space-y-4 border-b p-6">
                  <div className="relative">
                    <SearchIcon className="text-muted-foreground absolute top-1/2 left-3 size-4 -translate-y-1/2" />
                    <Input
                      value={query}
                      onChange={(event) => setQuery(event.target.value)}
                      placeholder={t.dataCenter.searchPlaceholder}
                      className="pl-9"
                    />
                  </div>
                  <div className="bg-muted inline-flex rounded-xl p-1">
                    <button
                      type="button"
                      onClick={() => setActiveTab("sources")}
                      className={cn(
                        "rounded-lg px-4 py-2 text-sm transition",
                        activeTab === "sources"
                          ? "bg-background shadow-sm"
                          : "text-muted-foreground",
                      )}
                    >
                      {t.dataCenter.allSources}
                    </button>
                    <button
                      type="button"
                      onClick={() => setActiveTab("uploads")}
                      className={cn(
                        "rounded-lg px-4 py-2 text-sm transition",
                        activeTab === "uploads"
                          ? "bg-background shadow-sm"
                          : "text-muted-foreground",
                      )}
                    >
                      {t.dataCenter.uploadedData}
                    </button>
                  </div>
                </div>

                <ScrollArea className="h-[calc(100vh-19rem)] xl:h-[calc(100vh-15rem)]">
                  <div className="space-y-2 p-4">
                    {error && (
                      <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-4 text-sm text-red-700">
                        {error instanceof Error
                          ? error.message
                          : "Failed to load data sources"}
                      </div>
                    )}
                    {isLoading && (
                      <div className="text-muted-foreground rounded-2xl border px-4 py-6 text-sm">
                        {t.common.loading}
                      </div>
                    )}
                    {visibleSources.map((source) => {
                      const Icon = iconOfSource(source);
                      const selected = selectedSource?.id === source.id;
                      return (
                        <button
                          key={source.id}
                          type="button"
                          onClick={() => setSelectedId(source.id)}
                          className={cn(
                            "w-full rounded-2xl border px-4 py-4 text-left transition",
                            selected
                              ? "border-primary/40 bg-primary/5 shadow-sm"
                              : "hover:bg-muted/60 bg-background",
                          )}
                        >
                          <div className="flex items-start gap-3">
                            <div className="bg-muted mt-0.5 rounded-xl p-2">
                              <Icon className="size-4" />
                            </div>
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center gap-2">
                                <div className="truncate font-medium">{source.name}</div>
                                <span className="bg-muted text-muted-foreground rounded-full px-2 py-0.5 text-[11px]">
                                  {labelOfType(source.type, t)}
                                </span>
                              </div>
                              <p className="text-muted-foreground mt-1 line-clamp-2 text-xs">
                                {source.description}
                              </p>
                            </div>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                </ScrollArea>
              </aside>

              <div className="grid min-h-0 grid-cols-1 xl:grid-cols-[minmax(0,1fr)_320px]">
                <div className="relative flex min-h-[32rem] flex-col items-center justify-center border-r px-8 py-10">
                  {selectedSource ? (
                    <div className="mx-auto flex w-full max-w-xl flex-col items-center text-center">
                      <div className="bg-primary/8 mb-6 rounded-[2rem] border border-dashed px-10 py-12">
                        <HardDriveDownloadIcon className="text-primary mx-auto size-12" />
                      </div>
                      <h2 className="text-2xl font-semibold">{selectedSource.name}</h2>
                      <p className="text-muted-foreground mt-3 max-w-md text-sm leading-6">
                        {selectedSource.description}
                      </p>
                      <div className="mt-6 flex flex-wrap items-center justify-center gap-2">
                        <span className="bg-muted rounded-full px-3 py-1 text-xs">
                          {labelOfType(selectedSource.type, t)}
                        </span>
                        <span className="bg-muted rounded-full px-3 py-1 text-xs">
                          {labelOfStatus(selectedSource.status, t)}
                        </span>
                        <span className="bg-muted rounded-full px-3 py-1 text-xs">
                          {selectedSource.owner_scope}
                        </span>
                      </div>
                      <div className="mt-8 flex flex-wrap items-center justify-center gap-3">
                        <Button
                          variant={
                            chatSelection.includes(selectedSource.id)
                              ? "secondary"
                              : "default"
                          }
                          onClick={handleUseForChat}
                        >
                          {chatSelection.includes(selectedSource.id)
                            ? t.dataCenter.selectedForChat
                            : t.dataCenter.selectForChat}
                        </Button>
                        <Button
                          variant="outline"
                          onClick={() => setDatabaseDialogOpen(true)}
                        >
                          {t.dataCenter.addDatabase}
                        </Button>
                      </div>
                      <p className="text-muted-foreground mt-6 text-xs">
                        {t.dataCenter.chatHint}
                      </p>
                    </div>
                  ) : (
                    <div className="mx-auto flex max-w-md flex-col items-center text-center">
                      <div className="bg-primary/8 mb-6 rounded-[2rem] border border-dashed px-10 py-12">
                        <DatabaseIcon className="text-primary mx-auto size-12" />
                      </div>
                      <h2 className="text-2xl font-semibold">{t.dataCenter.emptyTitle}</h2>
                      <p className="text-muted-foreground mt-3 text-sm leading-6">
                        {t.dataCenter.emptyDescription}
                      </p>
                      <Button
                        className="mt-8"
                        onClick={() => fileInputRef.current?.click()}
                        disabled={isUploading}
                      >
                        {isUploading ? t.uploads.uploading : t.dataCenter.emptyAction}
                      </Button>
                    </div>
                  )}
                </div>

                <aside className="bg-background/60 flex flex-col">
                  <div className="border-b px-6 py-5">
                    <div className="font-medium">{t.dataCenter.sourceDetail}</div>
                    <div className="text-muted-foreground mt-1 text-xs">
                      {t.dataCenter.mockHint}
                    </div>
                  </div>
                  <div className="space-y-5 p-6">
                    <div>
                      <div className="text-muted-foreground text-xs uppercase">
                        {t.dataCenter.selectedDataset}
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        {selectedChatSources.map((source) => (
                          <span
                            key={source.id}
                            className="bg-primary/8 text-primary rounded-full px-3 py-1 text-xs"
                          >
                            {source.name}
                          </span>
                        ))}
                      </div>
                    </div>

                    {selectedSource && (
                      <>
                        <DetailItem
                          label={t.dataCenter.sourceType}
                          value={labelOfType(selectedSourceDetail?.type ?? selectedSource.type, t)}
                        />
                        <DetailItem
                          label={t.dataCenter.sourceStatus}
                          value={labelOfStatus(selectedSourceDetail?.status ?? selectedSource.status, t)}
                        />
                        <DetailItem
                          label={t.dataCenter.sourceLocation}
                          value={selectedSourceDetail?.path ?? selectedSource.path ?? "-"}
                        />
                        <DetailItem
                          label={t.dataCenter.sourceUpdatedAt}
                          value={selectedSourceDetail?.updated_at ?? selectedSource.updated_at ?? "-"}
                        />
                        <DetailItem
                          label={t.dataCenter.sourceDescription}
                          value={
                            selectedSourceDetail?.description ||
                            selectedSource.description ||
                            t.dataCenter.noDescription
                          }
                        />
                      </>
                    )}
                  </div>
                </aside>
              </div>
            </div>
          </section>
        </div>
        <Dialog open={databaseDialogOpen} onOpenChange={setDatabaseDialogOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>{t.dataCenter.databaseComingSoonTitle}</DialogTitle>
              <DialogDescription>
                {t.dataCenter.databaseComingSoonDescription}
              </DialogDescription>
            </DialogHeader>
            <DialogFooter>
              <Button onClick={() => setDatabaseDialogOpen(false)}>
                {t.dataCenter.databaseComingSoonAction}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}

function DetailItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="space-y-2">
      <div className="text-muted-foreground text-xs uppercase">{label}</div>
      <div className="rounded-2xl border bg-white/80 px-4 py-3 text-sm leading-6">
        {value}
      </div>
    </div>
  );
}
