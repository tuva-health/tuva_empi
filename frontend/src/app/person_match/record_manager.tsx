"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { useRouter } from "next/navigation";
import { useAppStore } from "@/providers/app_store_provider";
import { LoaderCircle, FolderSymlink } from "lucide-react";
import { getRoute, Route } from "@/lib/routes";
import { Table, TableBody } from "@/components/ui/table";
import { PersonRow, RecordTableHeader } from "@/components/person-record";
import { MergePersonsModal } from "@/components/merge-persons-modal";
import {
  PersonRecordWithMetadata,
  PotentialMatchWithMetadata,
} from "@/lib/stores/person_match_slice";
import { isPotentialMatchUpdated } from "@/lib/utils";

export const RecordManager: React.FC = () => {
  const router = useRouter();

  const {
    matchMode,
    removePerson,
    selectSummary,
    currentPersons,
    openMergeModal,
    createNewPerson,
    personSummaries,
    isMergeModalOpen,
    potentialMatches,
    selectedPersonId,
    movePersonRecord,
    matchPersonRecords,
    currentPotentialMatches,
    potentialMatchSummaries,
    selectedPotentialMatchId,
    resetCurrentPotentialMatch,
  } = useAppStore((state) => state.personMatch);

  const potentialMatch =
    matchMode && selectedPotentialMatchId
      ? currentPotentialMatches[selectedPotentialMatchId]
      : undefined;
  const initialPotentialMatch =
    matchMode && selectedPotentialMatchId
      ? potentialMatches[selectedPotentialMatchId]
      : undefined;
  const persons =
    !matchMode && selectedPersonId && selectedPersonId in currentPersons
      ? [currentPersons[selectedPersonId]]
      : [];
  const loading = matchMode
    ? selectedPotentialMatchId && !potentialMatch
    : selectedPersonId && persons.length === 0;
  const renderedPersons = Object.values(
    matchMode
      ? ((potentialMatch as PotentialMatchWithMetadata)?.persons ?? [])
      : persons,
  );
  const isUpdated = isPotentialMatchUpdated(
    potentialMatch,
    initialPotentialMatch,
  );

  const handleReset = (): void => {
    if (potentialMatch) {
      resetCurrentPotentialMatch(potentialMatch.id);
    }
  };

  const handleSave = (): void => {
    if (potentialMatch) {
      matchPersonRecords(potentialMatch.id);

      const params: { matchMode?: string; id?: string } = {};

      if (matchMode) {
        params.matchMode = "true";
      }
      router.push(getRoute(Route.personMatch, undefined, params), {
        scroll: false,
      });
    }
  };

  const handleSummaryClick = (): void => {
    const summaries = Object.values(
      matchMode ? potentialMatchSummaries : personSummaries,
    );
    const id = summaries?.[0]?.id;

    if (!id) {
      return;
    }

    selectSummary(id);

    const params: { matchMode?: string; id: string } = { id };

    if (matchMode) {
      params.matchMode = matchMode.toString();
    }

    router.push(getRoute(Route.personMatch, undefined, params), {
      scroll: false,
    });
  };

  const handleRecordDrop = (
    personRecord: PersonRecordWithMetadata,
    toPersonId: string,
  ): void => {
    if (!matchMode || !potentialMatch) return;

    movePersonRecord(potentialMatch.id, personRecord, toPersonId);
  };

  return (
    <div className="flex flex-col w-full">
      <div className="flex flex-row-reverse items-center gap-4 p-3 border-b">
        {matchMode && !!potentialMatch?.id && (
          <div className="flex items-center gap-4 px-6">
            {isUpdated && (
              <Button className="h-10" variant="ghost" onClick={handleReset}>
                Cancel
              </Button>
            )}

            <Button
              className="h-10 disabled:cursor-not-allowed disabled:pointer-events-auto disabled:hover:bg-primary"
              disabled={!isUpdated}
              onClick={handleSave}
            >
              Save
            </Button>

            {matchMode && (
              <Button
                variant="outline"
                className="h-10 disabled:cursor-not-allowed disabled:pointer-events-auto disabled:hover:bg-transparent"
                onClick={openMergeModal}
                disabled={
                  !potentialMatch?.id ||
                  Object.keys(potentialMatch.persons).length < 2
                }
              >
                <FolderSymlink />
                Merge Persons
              </Button>
            )}
          </div>
        )}

        {!loading &&
          ((!matchMode && !selectedPersonId) ||
            (matchMode && !selectedPotentialMatchId)) && (
            <Button className="h-10" onClick={handleSummaryClick}>
              Next {matchMode ? "Match" : "Person"}
            </Button>
          )}

        <div className="h-10" />
      </div>

      {(matchMode && potentialMatch) || (!matchMode && persons.length > 0) ? (
        <div className="h-full mr-4 p-4 border-r">
          <div className="border rounded-md">
            <Table>
              <RecordTableHeader matchMode={matchMode} />
              <TableBody>
                {renderedPersons.map((person, ndx) => (
                  <PersonRow
                    key={person.id}
                    person={person}
                    ndx={ndx}
                    matchMode={matchMode}
                    removePerson={removePerson}
                    onRecordDrop={handleRecordDrop}
                    personsCount={renderedPersons.length}
                  />
                ))}
              </TableBody>
            </Table>
          </div>

          {matchMode && potentialMatch && (
            <Button
              className="mt-4"
              variant="outline"
              onClick={() => createNewPerson(potentialMatch.id)}
            >
              + Create New Person
            </Button>
          )}
        </div>
      ) : loading ? (
        <div className="flex items-center justify-center h-full w-full">
          <LoaderCircle className="animate-spin" />
        </div>
      ) : (
        <div className="flex flex-col items-center justify-center h-full w-[calc(100%-2rem)] m-4 bg-zinc-50 rounded-md">
          <p className="text-sm">
            Click the “Next {matchMode ? "match" : "person"}” button above
          </p>
          <p className="text-sm">
            or select a {matchMode ? "match" : "person"} on the left to begin
          </p>
        </div>
      )}

      {matchMode &&
        !!potentialMatch?.id &&
        Object.keys(potentialMatch.persons).length > 1 && (
          <MergePersonsModal
            isOpen={isMergeModalOpen}
            persons={Object.values(potentialMatch.persons)}
          />
        )}
    </div>
  );
};
