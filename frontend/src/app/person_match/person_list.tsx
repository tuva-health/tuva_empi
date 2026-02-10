"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { useRouter } from "next/navigation";
import { useAppStore } from "@/providers/app_store_provider";
import { PanelLeftClose, X } from "lucide-react";
import { getRoute, Route } from "@/lib/routes";
import { PersonSummary, PotentialMatchSummary } from "@/lib/api";
import { formatMatchProbability } from "@/lib/utils";
import MatchModeSwitch from "@/components/match-mode-switch";
import PersonListFilterDrawer from "@/components/person-list-filter-drawer";

export const PersonList: React.FC = () => {
  const router = useRouter();

  const {
    matchMode,
    searchTerms,
    selectSummary,
    toggleSidebar,
    personSummaries,
    clearSearchTerms,
    selectedPersonId,
    isSidebarOpen: isOpen,
    potentialMatchSummaries,
    selectedPotentialMatchId,
  } = useAppStore((state) => state.personMatch);

  const matchesTitle = isOpen ? "Matches" : "Match";
  const personsTitle = isOpen ? "Persons" : "Person";
  const listTitle = matchMode ? matchesTitle : personsTitle;
  const summaries = matchMode ? potentialMatchSummaries : personSummaries;

  const getPersonName = (s: PersonSummary | PotentialMatchSummary): string => {
    return [s.last_name, s.first_name].filter(Boolean).join(", ");
  };

  const getListItemTitle = (
    s: PersonSummary | PotentialMatchSummary,
  ): string | undefined => {
    return isOpen ? undefined : getPersonName(s);
  };

  const getPersonInitials = (s: PersonSummary): string => {
    return [s.last_name?.[0], s.first_name?.[0]]
      .filter(Boolean)
      .join(" ")
      .toUpperCase();
  };

  const handleSummaryClick = (id: string): void => {
    selectSummary(id);

    const params: { matchMode?: string; id: string } = { id };

    if (matchMode) {
      params.matchMode = matchMode.toString();
    }

    router.push(getRoute(Route.personMatch, undefined, params), {
      scroll: false,
    });
  };

  return (
    <div
      className={`flex flex-col gap-4 h-full pt-4 pb-6 border-r overflow-hidden ${
        isOpen ? "w-[312px] px-4" : "w-[60px] px-2"
      }`}
    >
      {/* Header */}
      <div className="flex flex-col gap-2">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => toggleSidebar()}
          className={`h-10 ${isOpen ? "w-full" : "w-10"}`}
        >
          <PanelLeftClose className={`h-4 w-4 ${isOpen ? "" : "rotate-180"}`} />

          {isOpen && <span className="text-sm">Collapse Section</span>}
        </Button>

        <hr />

        <div
          className={`flex items-center gap-2 ${isOpen ? "flex-row" : "flex-col"}`}
        >
          <PersonListFilterDrawer />

          <MatchModeSwitch isSidebarOpen={isOpen} />

          {Object.keys(searchTerms).length > 0 && (
            <Button
              variant="ghost"
              className="w-10 h-10"
              onClick={() => clearSearchTerms()}
            >
              <X className="h-4 w-4" />
            </Button>
          )}

          {isOpen && Object.keys(searchTerms).length === 0 && (
            <div className="h-10 w-10" />
          )}
        </div>

        <hr />
      </div>

      <p
        className={`text-primary scroll-m-20 text-sm font-semibold tracking-tight ${isOpen ? "" : "text-center tracking-tighter"}`}
      >
        {listTitle}
      </p>

      {/* List */}
      <ul
        className={`flex flex-col gap-3 overflow-y-auto [scrollbar-width:none] [-ms-overflow-style:none] [&::-webkit-scrollbar]:hidden`}
      >
        {Object.values(summaries).map((s) => {
          const selected = matchMode
            ? selectedPotentialMatchId === s.id
            : selectedPersonId === s.id;

          return (
            <li
              key={s.id}
              title={getListItemTitle(s)}
              className={`flex gap-1 rounded border cursor-pointer text-xs font-semibold ${selected ? "border-[2px] border-ring bg-light-blue" : ""} ${isOpen ? "flex-col w-full px-2 pt-[1px] pb-1" : "flex-row items-center justify-center w-10 h-10 min-h-10"}`}
              onClick={() => handleSummaryClick(s.id)}
            >
              {isOpen ? (
                <>
                  {matchMode && (
                    <p className="w-full py-1 border-b font-semibold text-xs text-muted-foreground">
                      {formatMatchProbability(
                        (s as PotentialMatchSummary).max_match_probability,
                      )}
                    </p>
                  )}

                  <div className="flex flex-col gap-1 py-2">
                    <p className="text-sm">{getPersonName(s)}</p>

                    <p className="text-xs text-muted-foreground">
                      {"{" + s.data_sources.join(", ") + "}"}
                    </p>
                  </div>
                </>
              ) : matchMode ? (
                formatMatchProbability(
                  (s as PotentialMatchSummary).max_match_probability,
                  true,
                )
              ) : (
                getPersonInitials(s)
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
};
