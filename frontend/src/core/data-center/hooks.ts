import { useQuery } from "@tanstack/react-query";

import {
  getDataSourceDetail,
  getEsIndexDetail,
  getEsIndexSamples,
  listDataSources,
  listEsIndices,
} from "./api";

export function useDataSources() {
  return useQuery({
    queryKey: ["data-center", "sources"],
    queryFn: listDataSources,
    refetchOnWindowFocus: false,
  });
}

export function useDataSourceDetail(sourceId: string | null | undefined) {
  return useQuery({
    queryKey: ["data-center", "sources", sourceId],
    queryFn: () => getDataSourceDetail(sourceId!),
    enabled: Boolean(sourceId),
    refetchOnWindowFocus: false,
  });
}


export function useEsIndices() {
  return useQuery({
    queryKey: ["data-center", "es", "indices"],
    queryFn: listEsIndices,
    refetchOnWindowFocus: false,
  });
}

export function useEsIndexDetail(indexName: string | null | undefined) {
  return useQuery({
    queryKey: ["data-center", "es", "indices", indexName],
    queryFn: () => getEsIndexDetail(indexName!),
    enabled: Boolean(indexName),
    refetchOnWindowFocus: false,
  });
}

export function useEsIndexSamples(
  indexName: string | null | undefined,
  size = 5,
) {
  return useQuery({
    queryKey: ["data-center", "es", "indices", indexName, "samples", size],
    queryFn: () => getEsIndexSamples(indexName!, size),
    enabled: Boolean(indexName),
    refetchOnWindowFocus: false,
  });
}