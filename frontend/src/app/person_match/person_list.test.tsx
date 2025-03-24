import React from "react";
import { render, screen } from "@testing-library/react";
import { PersonList } from "./person_list";
import { useAppStore } from "@/providers/app_store_provider";
import { PotentialMatchSummary } from "@/lib/api";

// Mock the next/navigation
jest.mock("next/navigation", () => ({
  useRouter: (): { push: jest.Mock } => ({
    push: jest.fn(),
  }),
}));

// Mock the app store
jest.mock("@/providers/app_store_provider");

describe("PersonList", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should display the max match probability for potential matches", (): void => {
    // Mock potential match with 85% match
    const mockPotentialMatch: PotentialMatchSummary = {
      id: "123",
      first_name: "John",
      last_name: "Doe",
      data_sources: ["source1"],
      max_match_probability: 0.85,
    };

    // Mock the app store
    (useAppStore as jest.Mock).mockImplementation((selector) => {
      if (typeof selector === "function") {
        const state = {
          personMatch: {
            matchMode: true,
            potentialMatchSummaries: { "123": mockPotentialMatch },
            selectedPotentialMatchId: null,
            personSummaries: {},
            selectedPersonId: null,
            selectSummary: jest.fn(),
            clearSearchTerms: jest.fn(),
            dataSources: [],
            searchTerms: {},
            updateSearchTerms: jest.fn(),
            fetchSummaries: jest.fn(),
          },
        };
        return selector(state);
      }
    });

    render(<PersonList />);

    // Check if the max match probability is displayed properly rounded
    expect(screen.getByText("85% match")).toBeInTheDocument();
  });

  it("should handle different max match probabilities", () => {
    // Mock potential matches with different probabilities
    const mockPotentialMatches: Record<string, PotentialMatchSummary> = {
      "123": {
        id: "123",
        first_name: "John",
        last_name: "Doe",
        data_sources: ["source1"],
        max_match_probability: 0.85,
      },
      "456": {
        id: "456",
        first_name: "Jane",
        last_name: "Smith",
        data_sources: ["source2"],
        max_match_probability: 0.92,
      },
    };

    // Mock the app store
    (useAppStore as jest.Mock).mockImplementation((selector) => {
      if (typeof selector === "function") {
        const state = {
          personMatch: {
            matchMode: true,
            potentialMatchSummaries: mockPotentialMatches,
            selectedPotentialMatchId: null,
            personSummaries: {},
            selectedPersonId: null,
            selectSummary: jest.fn(),
            clearSearchTerms: jest.fn(),
            dataSources: [],
            searchTerms: {},
            updateSearchTerms: jest.fn(),
            fetchSummaries: jest.fn(),
          },
        };
        return selector(state);
      }
    });

    render(<PersonList />);

    // Check if both max match probabilities are displayed properly rounded
    expect(screen.getByText("85% match")).toBeInTheDocument();
    expect(screen.getByText("92% match")).toBeInTheDocument();
  });

  it("should properly round max match probabilities", () => {
    // Mock potential matches with probabilities that need rounding
    const mockPotentialMatches: Record<string, PotentialMatchSummary> = {
      "123": {
        id: "123",
        first_name: "John",
        last_name: "Doe",
        data_sources: ["source1"],
        max_match_probability: 0.8567,
      },
      "456": {
        id: "456",
        first_name: "Jane",
        last_name: "Smith",
        data_sources: ["source2"],
        max_match_probability: 0.9999993121605751,
      },
    };

    // Mock the app store
    (useAppStore as jest.Mock).mockImplementation((selector) => {
      if (typeof selector === "function") {
        const state = {
          personMatch: {
            matchMode: true,
            potentialMatchSummaries: mockPotentialMatches,
            selectedPotentialMatchId: null,
            personSummaries: {},
            selectedPersonId: null,
            selectSummary: jest.fn(),
            clearSearchTerms: jest.fn(),
            dataSources: [],
            searchTerms: {},
            updateSearchTerms: jest.fn(),
            fetchSummaries: jest.fn(),
          },
        };
        return selector(state);
      }
    });

    render(<PersonList />);

    // Check if max match probabilities are rounded properly
    expect(screen.getByText("86% match")).toBeInTheDocument(); // 0.8567 rounded to 86%
    expect(screen.getByText("100% match")).toBeInTheDocument(); // 0.9999993121605751 rounded to 100%
  });

  it("should display 100% only for exact 1.0 match", () => {
    // Mock potential matches with exactly 1.0 max match probability
    const mockPotentialMatches: Record<string, PotentialMatchSummary> = {
      "123": {
        id: "123",
        first_name: "John",
        last_name: "Doe",
        data_sources: ["source1"],
        max_match_probability: 1.0,
      },
    };

    // Mock the app store
    (useAppStore as jest.Mock).mockImplementation((selector) => {
      if (typeof selector === "function") {
        const state = {
          personMatch: {
            matchMode: true,
            potentialMatchSummaries: mockPotentialMatches,
            selectedPotentialMatchId: null,
            personSummaries: {},
            selectedPersonId: null,
            selectSummary: jest.fn(),
            clearSearchTerms: jest.fn(),
            dataSources: [],
            searchTerms: {},
            updateSearchTerms: jest.fn(),
            fetchSummaries: jest.fn(),
          },
        };
        return selector(state);
      }
    });

    render(<PersonList />);

    // Check that 100% is displayed only for exact 1.0
    expect(screen.getByText("100% match")).toBeInTheDocument();
  });
});
