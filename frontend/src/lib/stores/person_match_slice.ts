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

// FIXME: Use separate state field for storing expanded records?

export interface PersonRecordWithMetadata extends PersonRecord {
  // Whether or not the full details of the PersonRecord are shown
  expanded: boolean;
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

    // NOTE: This nested format isn't the easiest to update (notice all the parameters required).
    // Perhaps we can have things more relational for updating and join them into a nested format
    // for rendering. But not sure the best approach for computing derived data with Zustand. See:
    // https://github.com/pmndrs/zustand/issues/108
    setPersonRecordExpanded: (
      personId: string,
      recordId: string,
      expanded: boolean,
      potentialMatchId?: string,
    ) => void;
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
        if (get().personMatch.matchMode) {
          set((state) => {
            state.personMatch.selectedPotentialMatchId = id;
          });
        } else {
          set((state) => {
            state.personMatch.selectedPersonId = id;
          });
        }
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
          // If it's a new Person without any records, don't upload it
          if (person.id === "" && person.records.length === 0) {
            continue;
          }

          const update: PersonUpdate = {
            new_person_record_ids: person.records.map((record) => record.id),
          };

          if (person.id) {
            update.id = person.id;
          }

          if (person.version) {
            update.version = person.version;
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
       * Expands an individual PersonRecord, depending on matchMode
       */
      setPersonRecordExpanded: (
        personId: string,
        recordId: string,
        expanded: boolean,
        potentialMatchId?: string,
      ): void => {
        set((state) => {
          if (state.personMatch.matchMode) {
            if (!potentialMatchId) {
              throw Error(
                "potentialMatchId is required when matchMode is true",
              );
            }

            for (const record of state.personMatch.currentPotentialMatches[
              potentialMatchId
            ].persons[personId].records) {
              if (record.id === recordId) {
                record.expanded = expanded;
              }
            }
          } else {
            for (const record of state.personMatch.currentPersons[personId]
              .records) {
              if (record.id === recordId) {
                record.expanded = expanded;
              }
            }
          }
        });
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
                      expanded: false,
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
                    expanded: false,
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
              records: person.records.map((r) => ({
                ...r,
                expanded: false,
              })),
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
            records: person.records.map((r) => ({
              ...r,
              expanded: false,
            })),
          };
        });
      },
    },
  });
