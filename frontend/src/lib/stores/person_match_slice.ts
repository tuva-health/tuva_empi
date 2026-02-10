import { StateCreator } from "zustand/vanilla";
import type {
  DataSource,
  Person,
  PersonRecord,
  PersonSummary,
  PersonUpdate,
  PotentialMatch,
  PotentialMatchSummary,
  SearchTerms,
} from "@/lib/api";
import * as api from "@/lib/api";
import { AppStore } from "./types";

export interface PersonRecordWithMetadata extends PersonRecord {
  // Store the highest match probability for this record
  highest_match_probability?: number;
  [key: string]: string | number | boolean | Date | undefined;
}

export interface PersonWithMetadata extends Person {
  records: PersonRecordWithMetadata[];
}

export interface PotentialMatchWithMetadata
  extends Omit<PotentialMatch, "persons"> {
  // Indexed by Person ID
  persons: Record<string, PersonWithMetadata>;
}

export interface PersonMatchState {
  personMatch: {
    dataSources: DataSource[];
    matchMode: boolean;
    searchTerms: SearchTerms;

    /**
     * PotentialMatch state
     */

    // Indexed by PotentialMatch ID
    potentialMatchSummaries: Record<string, PotentialMatchSummary>;
    // Indexed by PotentialMatch ID
    potentialMatches: Record<string, PotentialMatch | undefined>;
    // Indexed by PotentialMatch ID
    currentPotentialMatches: Record<string, PotentialMatchWithMetadata>;
    selectedPotentialMatchId: string | null;

    /**
     * Person state
     */

    // Indexed by Person ID
    personSummaries: Record<string, PersonSummary>;
    // Indexed by Person ID
    persons: Record<string, Person | undefined>;
    // Indexed by Person ID
    currentPersons: Record<string, PersonWithMetadata>;
    selectedPersonId: string | null;

    // Indexed by Record ID
    expandedRecords: Record<string, Record<string, PersonRecordWithMetadata>>;

    isSidebarOpen: boolean;
    isMergeModalOpen: boolean;
  };
}

export interface PersonMatchActions {
  personMatch: {
    fetchDataSources: () => Promise<void>;
    setMatchMode: (value: boolean) => void;

    updateSearchTerms: (key: string, value: string) => void;
    clearSearchTerms: () => void;
    fetchSummaries: () => Promise<void>;
    selectSummary: (id: string) => void;

    /**
     * PotentialMatch actions
     */

    fetchPotentialMatch: (id: string) => Promise<void>;
    resetCurrentPotentialMatch: (id: string) => void;

    /**
     * Person actions
     */

    fetchPerson: (id: string) => Promise<void>;
    resetCurrentPerson: (id: string) => void;

    movePersonRecord: (
      potentialMatchId: string,
      personRecord: PersonRecordWithMetadata,
      toPersonId: string,
    ) => void;

    matchPersonRecords: (potentialMatchId: string) => Promise<void>;

    /**
     * Creates a new person in the current potential match
     */
    createNewPerson: (potentialMatchId: string, merge?: boolean) => void;

    toggleExpandedRecord: (
      id: string,
      record: PersonRecordWithMetadata,
    ) => void;
    clearExpandedRecords: () => void;

    toggleSidebar: (isOpen?: boolean) => void;

    openMergeModal: () => void;
    closeMergeModal: () => void;
    mergePersons: (targetPersonId: string) => void;

    removePerson: (id: string) => void;
  };
}

export type PersonMatchSlice = PersonMatchState & PersonMatchActions;

export const defaultInitState: PersonMatchState = {
  personMatch: {
    dataSources: [],
    matchMode: false,
    // searchTerms are shared regardless of matchMode value
    searchTerms: {},

    // PotentialMatches are selected and updated independently of Persons
    potentialMatchSummaries: {},
    potentialMatches: {},
    currentPotentialMatches: {},
    selectedPotentialMatchId: null,

    personSummaries: {},
    persons: {},
    currentPersons: {},
    selectedPersonId: null,

    expandedRecords: {},
    isSidebarOpen: true,
    isMergeModalOpen: false,
  },
};

/**
 * Calculate highest match probabilities for all records in one pass
 * @param results The prediction results from a PotentialMatch
 * @returns A map of record IDs to their highest match probabilities
 */
export const calculateHighestMatchProbabilities = (
  results: PotentialMatch["results"],
): Record<string, number> => {
  if (!results?.length) return {};

  // Group results by record ID (both left and right sides)
  const recordResultsMap: Record<string, number[]> = {};

  // Process all results in a single pass
  for (const result of results) {
    // Add this result to the left record's list
    if (result.person_record_l_id) {
      if (!recordResultsMap[result.person_record_l_id]) {
        recordResultsMap[result.person_record_l_id] = [];
      }
      recordResultsMap[result.person_record_l_id].push(
        result.match_probability,
      );
    }

    // Add this result to the right record's list
    if (result.person_record_r_id) {
      if (!recordResultsMap[result.person_record_r_id]) {
        recordResultsMap[result.person_record_r_id] = [];
      }
      recordResultsMap[result.person_record_r_id].push(
        result.match_probability,
      );
    }
  }

  // Find the highest probability for each record
  const highestMatchProbabilities: Record<string, number> = {};

  for (const [recordId, probabilities] of Object.entries(recordResultsMap)) {
    if (probabilities.length > 0) {
      // Sort and take the highest
      highestMatchProbabilities[recordId] = Math.max(...probabilities);
    }
  }

  return highestMatchProbabilities;
};

export const createPersonMatchSlice =
  (
    initState: PersonMatchState,
  ): StateCreator<
    AppStore,
    [["zustand/immer", never], never],
    [],
    PersonMatchSlice
  > =>
  (set, get) => ({
    personMatch: {
      ...initState.personMatch,

      fetchDataSources: async (): Promise<void> => {
        const dataSources = await api.fetchDataSources();

        set((state) => {
          state.personMatch.dataSources = dataSources;
        });
      },

      setMatchMode: (value: boolean): void => {
        set((state) => {
          state.personMatch.matchMode = value;
        });
      },

      updateSearchTerms: (key: string, value: string): void => {
        set((state) => {
          state.personMatch.searchTerms[key] = value;
        });
      },

      clearSearchTerms: (): void => {
        set((state) => {
          state.personMatch.searchTerms = {};
        });
        get().personMatch.fetchSummaries();
      },

      /**
       * Fetch summaries for PotentialMatches and Persons at the same time.
       */
      fetchSummaries: async (): Promise<void> => {
        const potentialMatchSummaries = await api.fetchPotentialMatches(
          get().personMatch.searchTerms,
        );

        set((state) => {
          state.personMatch.potentialMatchSummaries = {};

          for (const summary of potentialMatchSummaries) {
            state.personMatch.potentialMatchSummaries[summary.id] = summary;
          }
        });

        // If search terms change, clear selected PotentialMatch
        const selectedPotentialMatchId =
          get().personMatch.selectedPotentialMatchId;
        if (
          selectedPotentialMatchId &&
          !(
            selectedPotentialMatchId in
            get().personMatch.potentialMatchSummaries
          )
        ) {
          set((state) => {
            state.personMatch.selectedPotentialMatchId = null;
          });
        }

        const personSummaries = await api.fetchPersons(
          get().personMatch.searchTerms,
        );

        set((state) => {
          state.personMatch.personSummaries = {};

          for (const summary of personSummaries) {
            state.personMatch.personSummaries[summary.id] = summary;
          }
        });

        // If search terms change, clear selected Person
        const selectedPersonId = get().personMatch.selectedPersonId;
        if (
          selectedPersonId &&
          !(selectedPersonId in get().personMatch.personSummaries)
        ) {
          set((state) => {
            state.personMatch.selectedPersonId = null;
          });
        }
      },

      /**
       * Select PotentialMatch or Person, depending on matchMode
       */
      selectSummary: (id: string): void => {
        set((state) => {
          const expandedRecords = state.personMatch.expandedRecords;

          if (get().personMatch.matchMode) {
            state.personMatch.selectedPotentialMatchId = id;
          } else {
            state.personMatch.selectedPersonId = id;
          }

          if (
            id in expandedRecords &&
            Object.keys(expandedRecords[id]).length > 0
          ) {
            state.personMatch.isSidebarOpen = false;
          }
        });
      },

      /**
       * Match PersonRecords (only used in matchMode for now)
       */
      matchPersonRecords: async (potentialMatchId: string): Promise<void> => {
        const potentialMatch =
          get().personMatch.currentPotentialMatches[potentialMatchId];
        const person_updates: PersonUpdate[] = [];

        if (potentialMatchId !== potentialMatch.id) {
          console.error("Failed to match PersonRecords");
          return;
        }

        for (const person of Object.values(potentialMatch.persons)) {
          const isNewPerson = person.id.startsWith("new-person-");

          // If it's a new Person without any records, don't upload it
          if (isNewPerson && person.records.length === 0) {
            continue;
          }

          const update: PersonUpdate = {
            new_person_record_ids: person.records.map((record) => record.id),
          };

          // Only include id and version for existing persons (not new ones)
          if (!isNewPerson) {
            if (person.id) {
              update.id = person.id;
            }
            if (person.version) {
              update.version = person.version;
            }
          }

          person_updates.push(update);
        }

        await api.matchPersonRecords(
          potentialMatch.id,
          potentialMatch.version,
          person_updates,
        );

        set((state) => {
          delete state.personMatch.potentialMatchSummaries[potentialMatch.id];
          delete state.personMatch.potentialMatches[potentialMatch.id];
          delete state.personMatch.currentPotentialMatches[potentialMatch.id];
          state.personMatch.selectedPotentialMatchId = null;
        });

        get().personMatch.fetchSummaries();
      },

      /**
       * PotentialMatch actions
       */

      /**
       * Fetches a single PotentialMatch in detail.
       */
      fetchPotentialMatch: async (id: string): Promise<void> => {
        const potentialMatch = await api.fetchPotentialMatch(id);

        if (!potentialMatch) {
          console.error("Failed to fetch PotentialMatch");
          return;
        }

        const potentialMatches = get().personMatch.potentialMatches;

        // Only update potentialMatches/currentPotentialMatches if PotentialMatch has
        // changed.
        if (
          !(potentialMatch.id in potentialMatches) ||
          (potentialMatches[potentialMatch.id] as PotentialMatch).version !==
            potentialMatch.version
        ) {
          // Calculate all highest match probabilities at once
          const highestMatchProbabilities = calculateHighestMatchProbabilities(
            potentialMatch.results,
          );

          set((state) => {
            state.personMatch.potentialMatches[potentialMatch.id] =
              potentialMatch;
            state.personMatch.currentPotentialMatches[potentialMatch.id] = {
              ...potentialMatch,
              persons: potentialMatch.persons.reduce(
                (
                  idToPersonMap: Record<string, PersonWithMetadata>,
                  person: Person,
                ) => {
                  const personWithMetadata = {
                    ...person,
                    records: person.records.map((r) => ({
                      ...r,
                      highest_match_probability:
                        highestMatchProbabilities[r.id],
                    })),
                  };

                  idToPersonMap[person.id] = personWithMetadata;

                  return idToPersonMap;
                },
                {},
              ),
            };
          });
        }
      },

      /**
       * Moves record from Person A to Person B (only used in matchMode for now)
       */
      movePersonRecord: (
        potentialMatchId: string,
        personRecord: PersonRecordWithMetadata,
        toPersonId: string,
      ): void => {
        const fromPersonId = personRecord.person_id;

        if (fromPersonId === toPersonId) {
          return;
        }

        set((state) => {
          const potentialMatch =
            state.personMatch.currentPotentialMatches[potentialMatchId];

          if (!potentialMatch) {
            console.error("Current PotentialMatch does not exist");
            return;
          }

          const persons = potentialMatch.persons;

          // Remove personRecord from its existing Person
          const fromPerson = persons[fromPersonId];

          fromPerson.records = fromPerson.records.filter(
            (record: PersonRecord) => record.id !== personRecord.id,
          );

          // Add personRecord to its new Person
          const toPerson = persons[toPersonId];

          toPerson.records.push({ ...personRecord, person_id: toPersonId });
        });
      },

      /**
       * Resets currently selected PotentialMatch to beginning state (before any records were moved).
       */
      resetCurrentPotentialMatch: (id: string): void => {
        set((state) => {
          const potentialMatch = state.personMatch.potentialMatches[id];

          if (!potentialMatch) {
            console.error("PotentialMatch does not exist");
            return;
          }

          // Calculate all highest match probabilities at once
          const highestMatchProbabilities = calculateHighestMatchProbabilities(
            potentialMatch.results,
          );

          state.personMatch.currentPotentialMatches[id] = {
            ...potentialMatch,
            persons: potentialMatch.persons.reduce(
              (
                idToPersonMap: Record<string, PersonWithMetadata>,
                person: Person,
              ) => {
                const personWithMetadata = {
                  ...person,
                  records: person.records.map((r) => ({
                    ...r,
                    highest_match_probability: highestMatchProbabilities[r.id],
                  })),
                };

                idToPersonMap[person.id] = personWithMetadata;

                return idToPersonMap;
              },
              {},
            ),
          };
        });
      },

      /**
       * Person actions
       */

      /**
       * Fetches a single Person in detail.
       */
      fetchPerson: async (id: string): Promise<void> => {
        const person = await api.fetchPerson(id);

        if (!person) {
          console.error("Failed to fetch Person");
          return;
        }

        const persons = get().personMatch.persons;

        // Only update persons/currentPersons if Person has
        // changed.
        if (
          !(person.id in persons) ||
          (persons[person.id] as Person).version !== person.version
        ) {
          set((state) => {
            state.personMatch.persons[person.id] = person;
            state.personMatch.currentPersons[person.id] = {
              ...person,
              records: person.records.map((r) => ({ ...r })),
            };
          });
        }
      },

      /**
       * Resets currently selected Person to beginning state (before any records were moved).
       */
      resetCurrentPerson: (id: string): void => {
        set((state) => {
          const person = state.personMatch.persons[id];

          if (!person) {
            console.error("Person does not exist");
            return;
          }

          state.personMatch.currentPersons[id] = {
            ...person,
            records: person.records.map((r) => ({ ...r })),
          };
        });
      },

      /**
       * Creates a new person in the current potential match
       */
      createNewPerson: (potentialMatchId: string, merge?: boolean): void => {
        set((state) => {
          const potentialMatch =
            state.personMatch.currentPotentialMatches[potentialMatchId];
          if (!potentialMatch) {
            console.error("Current PotentialMatch does not exist");
            return;
          }

          // Find the next available index
          const newPersonIds = Object.keys(potentialMatch.persons)
            .filter((id) => id.startsWith("new-person-"))
            .map((id) => parseInt(id.replace("new-person-", "")))
            .sort((a, b) => b - a);

          const nextIndex = newPersonIds.length > 0 ? newPersonIds[0] + 1 : 1;
          const newPersonId = `new-person-${nextIndex}`;

          potentialMatch.persons[newPersonId] = {
            id: newPersonId,
            created: new Date(),
            records: [],
          };

          if (merge) {
            const allRecords: PersonRecordWithMetadata[] = [];

            for (const person of Object.values(potentialMatch.persons)) {
              if (person.id !== newPersonId) {
                allRecords.push(...person.records);
                person.records = [];
              }
            }

            potentialMatch.persons[newPersonId].records.push(...allRecords);
          }
        });

        if (merge) {
          get().personMatch.closeMergeModal();
        }
      },

      toggleExpandedRecord: (
        id: string,
        record: PersonRecordWithMetadata,
      ): void => {
        set((state) => {
          const matchMode = state.personMatch.matchMode;
          const expandedRecords = state.personMatch.expandedRecords;

          const selectedSummaryId = matchMode
            ? state.personMatch.selectedPotentialMatchId
            : state.personMatch.selectedPersonId;

          if (!selectedSummaryId) {
            return;
          }

          const summaryExists = selectedSummaryId in expandedRecords;

          if (summaryExists) {
            if (id in expandedRecords[selectedSummaryId])
              delete state.personMatch.expandedRecords[selectedSummaryId][id];
            else {
              state.personMatch.expandedRecords[selectedSummaryId] = {
                ...state.personMatch.expandedRecords[selectedSummaryId],
                [id]: record,
              };
            }
          } else {
            state.personMatch.expandedRecords = {
              ...state.personMatch.expandedRecords,
              [selectedSummaryId]: {
                [id]: record,
              },
            };
          }

          if (
            Object.keys(state.personMatch.expandedRecords[selectedSummaryId])
              ?.length === 0
          ) {
            state.personMatch.isSidebarOpen = true;
          } else {
            state.personMatch.isSidebarOpen = false;
          }
        });
      },

      clearExpandedRecords: (): void => {
        set((state) => {
          const matchMode = state.personMatch.matchMode;
          const selectedPId = state.personMatch.selectedPersonId;
          const selectedPMId = state.personMatch.selectedPotentialMatchId;

          state.personMatch.isSidebarOpen = true;

          if (matchMode) {
            if (selectedPMId) {
              delete state.personMatch.expandedRecords[selectedPMId];
            }
          } else {
            if (selectedPId) {
              delete state.personMatch.expandedRecords[selectedPId];
            }
          }
        });
      },

      toggleSidebar: (isOpen?: boolean): void => {
        set((state) => {
          state.personMatch.isSidebarOpen =
            isOpen ?? !state.personMatch.isSidebarOpen;
        });
      },

      openMergeModal: (): void => {
        set((state) => {
          state.personMatch.isMergeModalOpen = true;
        });
      },

      closeMergeModal: (): void => {
        set((state) => {
          state.personMatch.isMergeModalOpen = false;
        });
      },

      mergePersons: (targetPersonId: string): void => {
        const { matchMode, selectedPotentialMatchId, currentPotentialMatches } =
          get().personMatch;

        if (
          !matchMode ||
          !selectedPotentialMatchId ||
          !(selectedPotentialMatchId in currentPotentialMatches)
        ) {
          console.error(
            "Cannot merge persons: not in match mode or no potential match selected",
          );
          return;
        }

        set((state) => {
          const currentMatch =
            state.personMatch.currentPotentialMatches[selectedPotentialMatchId];

          if (!currentMatch) {
            console.error("Current PotentialMatch does not exist");
            return;
          }

          // Collect all records from all persons except the target
          const allRecords: PersonRecordWithMetadata[] = [];
          // const personsToRemove: string[] = [];

          for (const person of Object.values(currentMatch.persons)) {
            if (person.id !== targetPersonId) {
              allRecords.push(
                ...person.records.map((r) => ({
                  ...r,
                  person_id: targetPersonId,
                })),
              );

              person.records = [];
              // personsToRemove.push(person.id);
            }
          }

          // Add all records to the target person
          const targetPerson = currentMatch.persons[targetPersonId];

          if (targetPerson) {
            targetPerson.records.push(...allRecords);
          }

          // Remove the other persons
          // for (const personId of personsToRemove) {
          //   delete currentMatch.persons[personId];
          // }
        });

        // TODO: Update the PotentialMatch API with the new records

        get().personMatch.closeMergeModal();
      },

      removePerson: (id: string): void => {
        set((state) => {
          const selectedPotentialMatchId =
            state.personMatch.selectedPotentialMatchId;

          if (
            !selectedPotentialMatchId ||
            !(
              selectedPotentialMatchId in
              state.personMatch.currentPotentialMatches
            )
          ) {
            return;
          }

          delete state.personMatch.currentPotentialMatches[
            selectedPotentialMatchId
          ].persons[id];
        });
      },
    },
  });
