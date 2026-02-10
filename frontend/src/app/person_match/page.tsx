"use client";

import React, { Suspense, useEffect, useState } from "react";
import NavBar from "@/app/nav_bar";
import { Tab } from "@/components/nav";
import { LoaderCircle } from "lucide-react";
import { RecordManager } from "@/app/person_match/record_manager";
import { PersonList } from "@/app/person_match/person_list";
import { useAppStore } from "@/providers/app_store_provider";
import { ReadonlyURLSearchParams, useSearchParams } from "next/navigation";
import RecordsComparison from "@/app/person_match/records_comparison";

const getMatchModeParam = (searchParams: ReadonlyURLSearchParams): boolean => {
  return searchParams.get("matchMode") === "true";
};

/**
 * This is necessary because NextJS throws an error if we use useSearchParams outside of a
 * Suspense component.
 */
const PersonMatchPageImpl: React.FC = () => {
  const searchParams = useSearchParams();

  const {
    matchMode,
    fetchPerson,
    setMatchMode,
    selectSummary,
    fetchSummaries,
    fetchDataSources,
    fetchPotentialMatch,
  } = useAppStore((state) => state.personMatch);

  const [loading, setLoading] = useState(true);

  // Set state based on page query parameters
  useEffect(() => {
    const resourceIdParam = searchParams.get("id");
    const matchModeParam = getMatchModeParam(searchParams);

    fetchDataSources();
    fetchSummaries();
    setMatchMode(matchModeParam);

    if (resourceIdParam) {
      selectSummary(resourceIdParam);
      if (matchModeParam) {
        fetchPotentialMatch(resourceIdParam);
      } else {
        fetchPerson(resourceIdParam);
      }
    }
  }, [
    searchParams,
    fetchDataSources,
    fetchSummaries,
    setMatchMode,
    selectSummary,
    fetchPotentialMatch,
    fetchPerson,
  ]);

  // We don't want the matchMode switch to toggle from off to on so we don't display the page
  // until the state has been set correctly from the URL params. I'm sure there is a better
  // way to do this.
  useEffect(() => {
    if (matchMode === getMatchModeParam(searchParams)) {
      setLoading(false);
    }
  }, [searchParams, matchMode]);

  return (
    <div className="flex flex-col w-full h-full">
      <NavBar selectedTab={Tab.personMatch} />

      {loading ? (
        <></>
      ) : (
        <>
          <div className="flex items-center justify-between w-full h-[60px] min-h-[60px] px-6 border-b border-muted-foreground">
            <h1 className="text-foreground scroll-m-20 text-[32px] font-extrabold tracking-tight">
              Person Match
            </h1>
          </div>

          <div className="grid grid-cols-[minmax(60px,max-content)_minmax(768px,1fr)_minmax(0px,max-content)] h-full w-full overflow-x-hidden">
            <PersonList />
            <RecordManager />
            <RecordsComparison />
          </div>
        </>
      )}
    </div>
  );
};

const PersonMatchPage: React.FC = () => {
  return (
    <Suspense
      fallback={
        <div className="flex flex-row items-center justify-center h-full w-full">
          <LoaderCircle className="animate-spin" />
        </div>
      }
    >
      <PersonMatchPageImpl />
    </Suspense>
  );
};

export default PersonMatchPage;
