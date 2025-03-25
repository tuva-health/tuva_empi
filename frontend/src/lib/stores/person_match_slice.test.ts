import { create } from "zustand";
import { immer } from "zustand/middleware/immer";
import {
  createPersonMatchSlice,
  defaultInitState,
  PersonWithMetadata,
  PersonRecordWithMetadata,
  calculateHighestMatchProbabilities,
} from "./person_match_slice";
import { PersonSummary, PotentialMatchSummary, PredictionResult } from "../api";
import * as api from "../api";
import { AppStore } from "./types";

// Mock the API module
jest.mock("../api");

describe("PersonMatchSlice", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should store potential match and person summaries on fetch", async () => {
    // Create a test store properly mimicking the AppStore structure
    const useTestStore = create<AppStore>()(
      immer((set, get, store) => ({
        ...defaultInitState,
        ...createPersonMatchSlice(defaultInitState)(set, get, store),
      })),
    );

    // Create a mock potential match summary with max_match_probability
    const mockSummary: PotentialMatchSummary = {
      id: "123",
      first_name: "John",
      last_name: "Doe",
      data_sources: ["source1"],
      max_match_probability: 0.85,
    };

    // Create a mock person summary
    const mockPersonSummary: PersonSummary = {
      id: "123",
      first_name: "John",
      last_name: "Doe",
      data_sources: ["source1"],
    };

    // Mock both API responses
    (api.fetchPotentialMatches as jest.Mock).mockResolvedValue([mockSummary]);
    (api.fetchPersons as jest.Mock).mockResolvedValue([mockPersonSummary]);

    // Call fetchSummaries
    await useTestStore.getState().personMatch.fetchSummaries();

    // Verify the max_match_probability is correctly stored in the state
    const state = useTestStore.getState();
    expect(state.personMatch.potentialMatchSummaries["123"]).toBeDefined();
    expect(state.personMatch.potentialMatchSummaries["123"].id).toBe("123");
    expect(state.personMatch.potentialMatchSummaries["123"].first_name).toBe(
      "John",
    );
    expect(state.personMatch.potentialMatchSummaries["123"].last_name).toBe(
      "Doe",
    );
    expect(
      state.personMatch.potentialMatchSummaries["123"].max_match_probability,
    ).toBe(0.85);

    // Verify the person summary is correctly stored in the state
    expect(state.personMatch.personSummaries["123"]).toBeDefined();
    expect(state.personMatch.personSummaries["123"].id).toBe("123");
    expect(state.personMatch.personSummaries["123"].first_name).toBe("John");
    expect(state.personMatch.personSummaries["123"].last_name).toBe("Doe");
    expect(state.personMatch.personSummaries["123"].data_sources).toEqual([
      "source1",
    ]);
  });

  it("should handle multiple potential matches and person summaries on fetch", async () => {
    // Create a test store properly mimicking the AppStore structure
    const useTestStore = create<AppStore>()(
      immer((set, get, store) => ({
        ...defaultInitState,
        ...createPersonMatchSlice(defaultInitState)(set, get, store),
      })),
    );

    // Create multiple mock summaries with different max match probabilities
    const mockPotentialMatchSummaries: PotentialMatchSummary[] = [
      {
        id: "123",
        first_name: "John",
        last_name: "Doe",
        data_sources: ["source1"],
        max_match_probability: 0.85,
      },
      {
        id: "456",
        first_name: "Jane",
        last_name: "Smith",
        data_sources: ["source2"],
        max_match_probability: 0.92,
      },
    ];

    // Create a mock person summary
    const mockPersonSummaries: PersonSummary[] = [
      {
        id: "123",
        first_name: "John",
        last_name: "Doe",
        data_sources: ["source1"],
      },
      {
        id: "456",
        first_name: "Jane",
        last_name: "Smith",
        data_sources: ["source2"],
      },
    ];

    // Mock both API responses
    (api.fetchPotentialMatches as jest.Mock).mockResolvedValue(
      mockPotentialMatchSummaries,
    );
    (api.fetchPersons as jest.Mock).mockResolvedValue(mockPersonSummaries);

    // Call fetchSummaries
    await useTestStore.getState().personMatch.fetchSummaries();

    // Verify all max match probabilities are correctly stored
    const state = useTestStore.getState();
    expect(state.personMatch.potentialMatchSummaries["123"]).toBeDefined();
    expect(state.personMatch.potentialMatchSummaries["123"].id).toBe("123");
    expect(state.personMatch.potentialMatchSummaries["123"].first_name).toBe(
      "John",
    );
    expect(state.personMatch.potentialMatchSummaries["123"].last_name).toBe(
      "Doe",
    );
    expect(
      state.personMatch.potentialMatchSummaries["123"].max_match_probability,
    ).toBe(0.85);
    expect(state.personMatch.potentialMatchSummaries["456"]).toBeDefined();
    expect(state.personMatch.potentialMatchSummaries["456"].id).toBe("456");
    expect(state.personMatch.potentialMatchSummaries["456"].first_name).toBe(
      "Jane",
    );
    expect(state.personMatch.potentialMatchSummaries["456"].last_name).toBe(
      "Smith",
    );
    expect(
      state.personMatch.potentialMatchSummaries["456"].max_match_probability,
    ).toBe(0.92);

    // Verify the person summary is correctly stored in the state
    expect(state.personMatch.personSummaries["123"]).toBeDefined();
    expect(state.personMatch.personSummaries["123"].id).toBe("123");
    expect(state.personMatch.personSummaries["123"].first_name).toBe("John");
    expect(state.personMatch.personSummaries["123"].last_name).toBe("Doe");
    expect(state.personMatch.personSummaries["123"].data_sources).toEqual([
      "source1",
    ]);
    expect(state.personMatch.personSummaries["456"]).toBeDefined();
    expect(state.personMatch.personSummaries["456"].id).toBe("456");
    expect(state.personMatch.personSummaries["456"].first_name).toBe("Jane");
    expect(state.personMatch.personSummaries["456"].last_name).toBe("Smith");
    expect(state.personMatch.personSummaries["456"].data_sources).toEqual([
      "source2",
    ]);
  });

  describe("matchPersonRecords", () => {
    it("should handle existing and new persons correctly", async () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      // Setup initial state with a potential match containing both existing and new persons
      const potentialMatchId = "match-123";
      const existingPerson: PersonWithMetadata = {
        id: "existing-123",
        version: 1,
        records: [
          { id: "record-1", expanded: false } as PersonRecordWithMetadata,
          { id: "record-2", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };
      const newPersonWithRecords: PersonWithMetadata = {
        id: "new-person-1",
        records: [
          { id: "record-3", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };
      const newPersonNoRecords: PersonWithMetadata = {
        id: "new-person-2",
        records: [],
        created: new Date(),
      };

      const mockResults: PredictionResult[] = [
        {
          id: "pred-1",
          person_record_l_id: "record-1",
          person_record_r_id: "record-2",
          match_probability: 0.9,
        },
      ];

      useTestStore.setState((state: AppStore) => {
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {
            [existingPerson.id]: existingPerson,
            [newPersonWithRecords.id]: newPersonWithRecords,
            [newPersonNoRecords.id]: newPersonNoRecords,
          },
          results: mockResults,
        };
      });

      // Mock API call
      (api.matchPersonRecords as jest.Mock).mockResolvedValue(undefined);

      // Call matchPersonRecords
      await useTestStore
        .getState()
        .personMatch.matchPersonRecords(potentialMatchId);

      // Verify API was called with correct parameters
      expect(api.matchPersonRecords).toHaveBeenCalledWith(potentialMatchId, 1, [
        {
          id: "existing-123",
          version: 1,
          new_person_record_ids: ["record-1", "record-2"],
        },
        {
          new_person_record_ids: ["record-3"],
        },
      ]);

      // Verify state cleanup
      const state = useTestStore.getState();
      expect(
        state.personMatch.potentialMatchSummaries[potentialMatchId],
      ).toBeUndefined();
      expect(
        state.personMatch.potentialMatches[potentialMatchId],
      ).toBeUndefined();
      expect(
        state.personMatch.currentPotentialMatches[potentialMatchId],
      ).toBeUndefined();
      expect(state.personMatch.selectedPotentialMatchId).toBeNull();
    });

    it("should handle error when potential match IDs don't match", async () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      // Setup initial state with mismatched IDs
      const potentialMatchId = "match-123";
      useTestStore.setState((state: AppStore) => {
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: "different-id",
          version: 1,
          persons: {},
          results: [],
        };
      });

      // Mock console.error
      const consoleSpy = jest.spyOn(console, "error");

      // Call matchPersonRecords
      await useTestStore
        .getState()
        .personMatch.matchPersonRecords(potentialMatchId);

      // Verify error was logged
      expect(consoleSpy).toHaveBeenCalledWith("Failed to match PersonRecords");

      // Verify API was not called
      expect(api.matchPersonRecords).not.toHaveBeenCalled();

      consoleSpy.mockRestore();
    });

    it("should allow dragging records into new person rows", async () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const potentialMatchId = "match-123";
      const existingPerson: PersonWithMetadata = {
        id: "existing-123",
        version: 1,
        records: [
          { id: "record-1", expanded: false } as PersonRecordWithMetadata,
          { id: "record-2", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };

      useTestStore.setState((state: AppStore) => {
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {
            [existingPerson.id]: existingPerson,
          },
          results: [],
        };
      });

      // Create a new person
      useTestStore.getState().personMatch.createNewPerson(potentialMatchId);

      // Move a record to the new person
      useTestStore.getState().personMatch.movePersonRecord(
        potentialMatchId,
        {
          ...existingPerson.records[0],
          person_id: existingPerson.id,
        } as PersonRecordWithMetadata,
        "new-person-1",
      );

      const state = useTestStore.getState();
      const persons =
        state.personMatch.currentPotentialMatches[potentialMatchId].persons;

      // Check record was moved correctly
      expect(persons["existing-123"].records.length).toBe(1);
      expect(persons["existing-123"].records[0].id).toBe("record-2");
      expect(persons["new-person-1"].records.length).toBe(1);
      expect(persons["new-person-1"].records[0].id).toBe("record-1");
      expect(persons["new-person-1"].records[0].person_id).toBe("new-person-1");
    });

    it("should handle canceling with new persons correctly", async () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const potentialMatchId = "match-123";
      const existingPerson: PersonWithMetadata = {
        id: "existing-123",
        version: 1,
        records: [
          { id: "record-1", expanded: false } as PersonRecordWithMetadata,
          { id: "record-2", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };

      // Set up initial state in both potentialMatches and currentPotentialMatches
      useTestStore.setState((state: AppStore) => {
        // Original state
        state.personMatch.potentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: [existingPerson],
          results: [],
        };

        // Current state with modifications
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {
            [existingPerson.id]: {
              ...existingPerson,
              records: [existingPerson.records[0]], // Moved one record out
            },
            "new-person-1": {
              id: "new-person-1",
              created: new Date(),
              records: [
                {
                  ...existingPerson.records[1],
                  person_id: "new-person-1",
                } as PersonRecordWithMetadata,
              ],
            },
          },
          results: [],
        };
      });

      // Reset the current potential match
      useTestStore
        .getState()
        .personMatch.resetCurrentPotentialMatch(potentialMatchId);

      const state = useTestStore.getState();
      const currentMatch =
        state.personMatch.currentPotentialMatches[potentialMatchId];

      // Verify state was reset correctly
      expect(Object.keys(currentMatch.persons).length).toBe(1);
      expect(currentMatch.persons["existing-123"]).toBeDefined();
      expect(currentMatch.persons["existing-123"].records.length).toBe(2);
      expect(currentMatch.persons["new-person-1"]).toBeUndefined();
    });
  });

  describe("createNewPerson", () => {
    it("should generate correct pseudo IDs for new persons", async () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const potentialMatchId = "match-123";
      useTestStore.setState((state: AppStore) => {
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {},
          results: [],
        };
      });

      // Create three new persons
      useTestStore.getState().personMatch.createNewPerson(potentialMatchId);
      useTestStore.getState().personMatch.createNewPerson(potentialMatchId);
      useTestStore.getState().personMatch.createNewPerson(potentialMatchId);

      const state = useTestStore.getState();
      const persons =
        state.personMatch.currentPotentialMatches[potentialMatchId].persons;

      expect(persons["new-person-1"]).toBeDefined();
      expect(persons["new-person-2"]).toBeDefined();
      expect(persons["new-person-3"]).toBeDefined();
      expect(Object.keys(persons).length).toBe(3);
    });

    it("should handle non-existent potential match", () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const consoleSpy = jest.spyOn(console, "error");
      useTestStore.getState().personMatch.createNewPerson("non-existent-match");

      expect(consoleSpy).toHaveBeenCalledWith(
        "Current PotentialMatch does not exist",
      );
      consoleSpy.mockRestore();
    });
  });

  describe("movePersonRecord", () => {
    it("should allow moving records between existing persons", () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const potentialMatchId = "match-123";
      const person1: PersonWithMetadata = {
        id: "person-1",
        version: 1,
        records: [
          { id: "record-1", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };
      const person2: PersonWithMetadata = {
        id: "person-2",
        version: 1,
        records: [],
        created: new Date(),
      };

      useTestStore.setState((state: AppStore) => {
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {
            [person1.id]: person1,
            [person2.id]: person2,
          },
          results: [],
        };
      });

      // Move record from person1 to person2
      useTestStore.getState().personMatch.movePersonRecord(
        potentialMatchId,
        {
          ...person1.records[0],
          person_id: person1.id,
        } as PersonRecordWithMetadata,
        person2.id,
      );

      const state = useTestStore.getState();
      const persons =
        state.personMatch.currentPotentialMatches[potentialMatchId].persons;

      expect(persons[person1.id].records.length).toBe(0);
      expect(persons[person2.id].records.length).toBe(1);
      expect(persons[person2.id].records[0].id).toBe("record-1");
      expect(persons[person2.id].records[0].person_id).toBe(person2.id);
    });

    it("should handle non-existent potential match", () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const consoleSpy = jest.spyOn(console, "error");
      useTestStore
        .getState()
        .personMatch.movePersonRecord(
          "non-existent-match",
          { id: "record-1", person_id: "person-1" } as PersonRecordWithMetadata,
          "person-2",
        );

      expect(consoleSpy).toHaveBeenCalledWith(
        "Current PotentialMatch does not exist",
      );
      consoleSpy.mockRestore();
    });

    it("should do nothing when moving to same person", () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const potentialMatchId = "match-123";
      const person: PersonWithMetadata = {
        id: "person-1",
        version: 1,
        records: [
          { id: "record-1", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };

      useTestStore.setState((state: AppStore) => {
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {
            [person.id]: person,
          },
          results: [],
        };
      });

      // Try to move record to same person
      useTestStore.getState().personMatch.movePersonRecord(
        potentialMatchId,
        {
          ...person.records[0],
          person_id: person.id,
        } as PersonRecordWithMetadata,
        person.id,
      );

      const state = useTestStore.getState();
      const persons =
        state.personMatch.currentPotentialMatches[potentialMatchId].persons;

      expect(persons[person.id].records.length).toBe(1);
      expect(persons[person.id].records[0].id).toBe("record-1");
    });
  });

  describe("resetCurrentPotentialMatch", () => {
    it("should handle canceling with new persons correctly", async () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const potentialMatchId = "match-123";
      const existingPerson: PersonWithMetadata = {
        id: "existing-123",
        version: 1,
        records: [
          { id: "record-1", expanded: false } as PersonRecordWithMetadata,
          { id: "record-2", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };

      // Set up initial state in both potentialMatches and currentPotentialMatches
      useTestStore.setState((state: AppStore) => {
        // Original state
        state.personMatch.potentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: [existingPerson],
          results: [],
        };

        // Current state with modifications
        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {
            [existingPerson.id]: {
              ...existingPerson,
              records: [existingPerson.records[0]], // Moved one record out
            },
            "new-person-1": {
              id: "new-person-1",
              created: new Date(),
              records: [
                {
                  ...existingPerson.records[1],
                  person_id: "new-person-1",
                } as PersonRecordWithMetadata,
              ],
            },
          },
          results: [],
        };
      });

      // Reset the current potential match
      useTestStore
        .getState()
        .personMatch.resetCurrentPotentialMatch(potentialMatchId);

      const state = useTestStore.getState();
      const currentMatch =
        state.personMatch.currentPotentialMatches[potentialMatchId];

      expect(Object.keys(currentMatch.persons).length).toBe(1);
      expect(currentMatch.persons["existing-123"]).toBeDefined();
      expect(currentMatch.persons["existing-123"].records.length).toBe(2);
      expect(currentMatch.persons["new-person-1"]).toBeUndefined();
    });

    it("should handle non-existent potential match", () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const consoleSpy = jest.spyOn(console, "error");
      useTestStore
        .getState()
        .personMatch.resetCurrentPotentialMatch("non-existent-match");

      expect(consoleSpy).toHaveBeenCalledWith("PotentialMatch does not exist");
      consoleSpy.mockRestore();
    });

    it("should preserve match probabilities when resetting", () => {
      const useTestStore = create<AppStore>()(
        immer((set, get, store) => ({
          ...defaultInitState,
          ...createPersonMatchSlice(defaultInitState)(set, get, store),
        })),
      );

      const potentialMatchId = "match-123";
      const existingPerson: PersonWithMetadata = {
        id: "existing-123",
        version: 1,
        records: [
          { id: "record-1", expanded: false } as PersonRecordWithMetadata,
          { id: "record-2", expanded: false } as PersonRecordWithMetadata,
        ],
        created: new Date(),
      };

      const mockResults = [
        {
          id: "pred-1",
          person_record_l_id: "record-1",
          person_record_r_id: "record-2",
          match_probability: 0.9,
        },
      ];

      useTestStore.setState((state: AppStore) => {
        state.personMatch.potentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: [existingPerson],
          results: mockResults,
        };

        state.personMatch.currentPotentialMatches[potentialMatchId] = {
          id: potentialMatchId,
          version: 1,
          persons: {
            [existingPerson.id]: {
              ...existingPerson,
              records: [
                {
                  ...existingPerson.records[0],
                  highest_match_probability: 0.5,
                },
                {
                  ...existingPerson.records[1],
                  highest_match_probability: 0.5,
                },
              ],
            },
          },
          results: mockResults,
        };
      });

      useTestStore
        .getState()
        .personMatch.resetCurrentPotentialMatch(potentialMatchId);

      const state = useTestStore.getState();
      const currentMatch =
        state.personMatch.currentPotentialMatches[potentialMatchId];
      const records = currentMatch.persons["existing-123"].records;

      expect(records[0].highest_match_probability).toBe(0.9);
      expect(records[1].highest_match_probability).toBe(0.9);
    });
  });
});

describe("calculateHighestMatchProbabilities", () => {
  it("should calculate highest match probabilities for each record", () => {
    const results = [
      {
        id: "pred1",
        person_record_l_id: "record1",
        person_record_r_id: "record2",
        match_probability: 0.8,
      },
      {
        id: "pred2",
        person_record_l_id: "record1",
        person_record_r_id: "record3",
        match_probability: 0.9,
      },
      {
        id: "pred3",
        person_record_l_id: "record2",
        person_record_r_id: "record3",
        match_probability: 0.7,
      },
    ];

    const probabilities = calculateHighestMatchProbabilities(results);

    expect(probabilities).toEqual({
      record1: 0.9, // Highest between 0.8 and 0.9
      record2: 0.8, // Highest between 0.8 and 0.7
      record3: 0.9, // Highest between 0.9 and 0.7
    });
  });
});
