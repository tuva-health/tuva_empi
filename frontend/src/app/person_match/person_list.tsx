"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { useRouter } from "next/navigation";
import { useAppStore } from "@/providers/app_store_provider";
import { Filter, X } from "lucide-react";
import { getRoute, Route } from "@/lib/routes";
import {
  Drawer,
  DrawerClose,
  DrawerContent,
  DrawerFooter,
  DrawerHeader,
  DrawerTitle,
  DrawerDescription,
  DrawerTrigger,
} from "@/components/ui/drawer";
import { Combobox } from "@/components/combobox";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { PotentialMatchSummary } from "@/lib/api";

// Format match probability for display
const formatMatchProbability = (probability: number): string => {
  const displayProbability = Math.round(probability * 100);
  return `${displayProbability}% match`;
};

const PersonListFilterDrawer: React.FC = () => {
  const dataSources = useAppStore((state) => state.personMatch.dataSources);
  const dataSourceOptions = dataSources.map((dataSource) => ({
    value: dataSource.name,
    label: dataSource.name,
  }));
  const searchTerms = useAppStore((state) => state.personMatch.searchTerms);
  const updateSearchTerms = useAppStore(
    (state) => state.personMatch.updateSearchTerms,
  );
  const clearSearchTerms = useAppStore(
    (state) => state.personMatch.clearSearchTerms,
  );
  const fetchSummaries = useAppStore(
    (state) => state.personMatch.fetchSummaries,
  );

  return (
    <Drawer direction="left" handleOnly={true}>
      <DrawerTrigger asChild>
        <Button variant="outline" className="w-full">
          <Filter />
          Search & Filter
        </Button>
      </DrawerTrigger>
      <DrawerContent className="h-full max-w-sm border-r justify-between p-6">
        <div className="h-full w-full flex flex-col relative gap-4">
          <DrawerClose>
            <X className="absolute h-4 w-4 top-0 right-0" />
          </DrawerClose>
          <DrawerHeader>
            <DrawerTitle>Filter Persons</DrawerTitle>
            <DrawerDescription className="text-white">
              Narrow down persons by using the filters below.
            </DrawerDescription>
          </DrawerHeader>
          <div className="w-full flex flex-col gap-2">
            <Label
              htmlFor="data-sources-filter"
              className="text-sm font-medium"
            >
              Data Sources
            </Label>
            <Combobox
              id="data-sources-filter"
              items={dataSourceOptions}
              placeholder="All data sources"
              onChange={(value: string) =>
                updateSearchTerms("data_source", value)
              }
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="first-name-filter" className="text-sm font-medium">
              First Name
            </Label>
            <Input
              id="first-name-filter"
              value={searchTerms.first_name ?? ""}
              onChange={(e) => updateSearchTerms("first_name", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="last-name-filter" className="text-sm font-medium">
              Last Name
            </Label>
            <Input
              id="last-name-filter"
              value={searchTerms.last_name ?? ""}
              onChange={(e) => updateSearchTerms("last_name", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="birth-date-filter" className="text-sm font-medium">
              Birth Date
            </Label>
            <Input
              id="birth-date-filter"
              value={searchTerms.birth_date ?? ""}
              onChange={(e) => updateSearchTerms("birth_date", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="pid-filter" className="text-sm font-medium">
              Person ID
            </Label>
            <Input
              id="pid-filter"
              value={searchTerms.person_id ?? ""}
              onChange={(e) => updateSearchTerms("person_id", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="spid-filter" className="text-sm font-medium">
              Source Person ID
            </Label>
            <Input
              id="spid-filter"
              value={searchTerms.source_person_id ?? ""}
              onChange={(e) =>
                updateSearchTerms("source_person_id", e.target.value)
              }
            />
          </div>
        </div>
        <DrawerFooter className="flex flex-row justify-between">
          <Button variant="ghost" onClick={() => clearSearchTerms()}>
            Clear Filters
          </Button>
          <div className="flex flex-row gap-2">
            <DrawerClose className="inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 hover:bg-accent hover:text-accent-foreground h-9 px-4 py-2">
              Cancel
            </DrawerClose>
            <DrawerClose
              onClick={() => fetchSummaries()}
              className="inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 bg-primary text-primary-foreground shadow hover:bg-primary/90 h-9 px-4 py-2"
            >
              Filter
            </DrawerClose>
          </div>
        </DrawerFooter>
      </DrawerContent>
    </Drawer>
  );
};

export const PersonList: React.FC = () => {
  const router = useRouter();
  const matchMode = useAppStore((state) => state.personMatch.matchMode);
  const potentialMatchSummaries = useAppStore(
    (state) => state.personMatch.potentialMatchSummaries,
  );
  const selectedPotentialMatchId = useAppStore(
    (state) => state.personMatch.selectedPotentialMatchId,
  );
  const personSummaries = useAppStore(
    (state) => state.personMatch.personSummaries,
  );
  const selectedPersonId = useAppStore(
    (state) => state.personMatch.selectedPersonId,
  );
  const selectSummary = useAppStore((state) => state.personMatch.selectSummary);
  const clearSearchTerms = useAppStore(
    (state) => state.personMatch.clearSearchTerms,
  );

  return (
    <div className="flex flex-col pt-4 pr-5 pb-6 gap-4 border-r-[1px] w-[340px] h-full">
      {/* Header */}
      <div className="flex flex-col gap-2">
        <div className="flex flex-row h-[40px] items-center">
          <h3 className="text-primary scroll-m-20 text-xl font-semibold tracking-tight">
            {matchMode ? "Potential Matches" : "Persons"}
          </h3>
        </div>
        <div className="flex flex-row w-full gap-2 pb-4 border-b-[1px]">
          <PersonListFilterDrawer />
          <Button variant="ghost" onClick={() => clearSearchTerms()}>
            Clear
          </Button>
        </div>
      </div>

      {/* List */}
      <ul className="flex flex-col overflow-y-auto gap-3">
        {Object.values(
          matchMode ? potentialMatchSummaries : personSummaries,
        ).map((s) => {
          const selected = matchMode
            ? selectedPotentialMatchId === s.id
            : selectedPersonId === s.id;

          return (
            <li
              key={s.id}
              className={`flex flex-col w-full rounded border gap-1 pt-[1px] pb-1 cursor-pointer ${selected ? "border-[2px] border-ring" : ""}`}
              onClick={() => {
                selectSummary(s.id);

                const params: { matchMode?: string; id: string } = {
                  id: s.id,
                };

                if (matchMode) {
                  params.matchMode = matchMode.toString();
                }

                router.push(getRoute(Route.personMatch, undefined, params), {
                  scroll: false,
                });
              }}
            >
              {matchMode ? (
                <div className="flex flex-row h-[23px] w-full px-2">
                  <div className="flex flex-row w-full border-b items-center">
                    <p className="text-xs">
                      {formatMatchProbability(
                        (s as PotentialMatchSummary).max_match_probability,
                      )}
                    </p>
                  </div>
                </div>
              ) : (
                <></>
              )}
              <ul className="flex flex-col w-full">
                <li className="flex flex-col py-2 pl-2">
                  <p className="text-sm">{s.last_name + ", " + s.first_name}</p>
                  <p className="text-xs text-muted-foreground">
                    {"{" + s.data_sources.join(", ") + "}"}
                  </p>
                </li>
              </ul>
            </li>
          );
        })}
      </ul>
    </div>
  );
};
