"use client";

import { ChevronRightIcon } from "lucide-react";
import Link from "next/link";

import { Button } from "@/components/ui/button";
import { FlickeringGrid } from "@/components/ui/flickering-grid";
import Galaxy from "@/components/ui/galaxy";
import { WordRotate } from "@/components/ui/word-rotate";
import { cn } from "@/lib/utils";

export function Hero({ className }: { className?: string }) {
  return (
    <div
      className={cn(
        "flex size-full flex-col items-center justify-center",
        className,
      )}
    >
      <div className="absolute inset-0 z-0 bg-[radial-gradient(circle_at_top,#b9ddff_0%,#e9f6ff_52%,#d5ebff_100%)]">
        <Galaxy
          mouseRepulsion={false}
          starSpeed={0.2}
          density={0.6}
          glowIntensity={0.2}
          twinkleIntensity={0.18}
          speed={0.35}
        />
      </div>
      <FlickeringGrid
        className="absolute inset-0 z-0 translate-y-8 opacity-60"
        squareSize={4}
        gridGap={4}
        color={"#1677c8"}
        maxOpacity={0.18}
        flickerChance={0.16}
      />
      <div className="container-md relative z-10 mx-auto flex h-screen flex-col items-center justify-center">
        <h1 className="flex items-center gap-2 text-4xl font-bold text-[#0b4f88] md:text-6xl">
          <WordRotate
            words={[
              "城市数据汇聚",
              "多源数据分析",
              "异常事件识别",
              "城市运行洞察",
              "专题报告生成",
              "辅助决策支撑",
            ]}
          />{" "}
          <div>平台</div>
        </h1>
        <p
          className="mt-8 scale-105 text-center text-2xl text-shadow-sm"
          style={{ color: "rgb(74,110,145)" }}
        >
          面向中国移动城市超脑建设场景打造的城市数据分析门户，支持
          <br />
          网络流量、时空轨迹、地理空间、遥感影像、统计年鉴等多源数据的
          <br />
          统一接入、专题洞察、异常识别与辅助决策输出。
        </p>
        <Link href="/workspace/chats/new">
          <Button className="size-lg mt-8 scale-108" size="lg">
            <span className="text-md">启动城市超脑</span>
            <ChevronRightIcon className="size-4" />
          </Button>
        </Link>
      </div>
    </div>
  );
}
