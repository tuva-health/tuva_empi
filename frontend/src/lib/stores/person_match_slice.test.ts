import { create } from "zustand";
import { immer } from "zustand/middleware/immer";
import {
  createPersonMatchSlice,
  defaultInitState,
  calculateHighestMatchProbabilities,
} from "./person_match_slice";
import { PersonSummary, PotentialMatchSummary } from "../api";
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
