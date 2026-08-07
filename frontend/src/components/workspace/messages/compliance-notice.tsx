import { ChevronDownIcon, ShieldAlertIcon, ShieldCheckIcon } from "lucide-react";
import { useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { useI18n } from "@/core/i18n/hooks";
import type {
  ComplianceAction,
  ComplianceDisposition,
  ComplianceGate,
  ComplianceScene,
  ComplianceViolationType,
} from "@/core/threads/compliance";
import { cn } from "@/lib/utils";

/**
 * Explains a compliance disposition attached to an assistant message.
 *
 * Without this the user just sees the answer change — or a Chinese notice glued
 * to the bottom of the markdown, indistinguishable from model output. The
 * structured fields (violation type, disposition, legal basis, audit reference)
 * already reach the browser and were being thrown away.
 *
 * Tone follows `disposition.mutated`, not merely "was anything flagged".
 * SceneResolver and the policy matrix can legitimately allow, warn, transform,
 * or refuse; the structured action is the durable source of truth.
 */
export function ComplianceNotice({
  className,
  disposition,
}: {
  className?: string;
  disposition: ComplianceDisposition;
}) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);

  const violationLabel = (code: string) =>
    // Fall through to the raw code rather than rendering blank: the detector set
    // is expected to grow to violation types 1-7.
    t.compliance.violationTypes[code as ComplianceViolationType] ?? code;
  const actionLabel = (code: string) =>
    t.compliance.actions[code as ComplianceAction] ?? code;

  const passed = disposition.checks.every((check) => check.status !== "handled");
  const StatusIcon = passed ? ShieldCheckIcon : ShieldAlertIcon;

  return (
    <Alert
      variant={disposition.mutated ? "destructive" : "default"}
      className={cn("border-border/60 bg-muted/40", className)}
    >
      {/* Must be a direct child of Alert — alertVariants keys its grid off has-[>svg]. */}
      <StatusIcon />
      <AlertTitle>
        {disposition.mutated
          ? t.compliance.titleBlocked
          : passed
            ? t.compliance.titlePassed
            : t.compliance.title}
      </AlertTitle>
      <AlertDescription className="w-full">
        <div className="flex flex-wrap items-center gap-1.5">
          {disposition.violationTypes.length > 0 ? (
            disposition.violationTypes.map((code) => (
              <Badge
                key={`v-${code}`}
                variant="secondary"
                className="rounded px-1.5 py-0.5 text-[10px] font-normal"
              >
                {violationLabel(code)}
              </Badge>
            ))
          ) : (
            <Badge
              variant="secondary"
              className="rounded px-1.5 py-0.5 text-[10px] font-normal"
            >
              {t.compliance.noRisk}
            </Badge>
          )}
          {disposition.actions.map((code) => (
            <Badge
              key={`a-${code}`}
              variant="outline"
              className="rounded px-1.5 py-0.5 text-[10px] font-normal"
            >
              {actionLabel(code)}
            </Badge>
          ))}
        </div>

        {/* w-full is required throughout: AlertDescription is
            `grid justify-items-start`, so children otherwise shrink to the
            width of the trigger text. */}
        {disposition.checks.length > 0 && (
          <Collapsible open={open} onOpenChange={setOpen} className="w-full">
            <CollapsibleTrigger className="text-muted-foreground hover:text-foreground mt-1.5 flex items-center gap-1 text-xs">
              {open ? t.compliance.hideDetails : t.compliance.showDetails}
              <ChevronDownIcon
                className={cn(
                  "size-3 transition-transform",
                  open && "rotate-180",
                )}
              />
            </CollapsibleTrigger>
            <CollapsibleContent className="mt-2">
              <div className="divide-border/60 divide-y">
                {disposition.checks.map((check, index) => (
                  <section
                    key={`${check.gate ?? "gate"}-${index}`}
                    className="py-2 first:pt-0 last:pb-0"
                  >
                    <div className="mb-2 flex flex-wrap items-center gap-2 text-xs font-medium">
                      <span>
                        {check.gate
                          ? (t.compliance.gates[
                              check.gate as ComplianceGate
                            ] ?? check.gate)
                          : t.compliance.gateLabel}
                      </span>
                      <Badge
                        variant="outline"
                        className="rounded px-1.5 py-0.5 text-[10px] font-normal"
                      >
                        {t.compliance.checkStatuses[check.status]}
                      </Badge>
                    </div>
                    <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
                      <dt className="text-muted-foreground">
                        {t.compliance.violationTypesLabel}
                      </dt>
                      <dd>
                        {check.violationTypes.length > 0
                          ? check.violationTypes.map(violationLabel).join("、")
                          : t.compliance.noRisk}
                      </dd>
                      <dt className="text-muted-foreground">
                        {t.compliance.actionsLabel}
                      </dt>
                      <dd>{check.actions.map(actionLabel).join("、")}</dd>
                      <dt className="text-muted-foreground">
                        {t.compliance.basisLabel}
                      </dt>
                      <dd>
                        {check.basis.length > 0 ? (
                          <ul className="list-inside list-disc">
                            {check.basis.map((clause) => (
                              <li key={clause}>{clause}</li>
                            ))}
                          </ul>
                        ) : (
                          t.compliance.noBasis
                        )}
                      </dd>
                      {check.scene && (
                        <>
                          <dt className="text-muted-foreground">
                            {t.compliance.sceneLabel}
                          </dt>
                          <dd>
                            {t.compliance.scenes[
                              check.scene as ComplianceScene
                            ] ?? check.scene}
                          </dd>
                        </>
                      )}
                      {check.auditRef && (
                        <>
                          <dt className="text-muted-foreground">
                            {t.compliance.auditRefLabel}
                          </dt>
                          <dd>
                            <code className="font-mono text-[11px]">
                              {check.auditRef}
                            </code>
                          </dd>
                        </>
                      )}
                    </dl>
                  </section>
                ))}
              </div>
            </CollapsibleContent>
          </Collapsible>
        )}
      </AlertDescription>
    </Alert>
  );
}
