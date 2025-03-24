import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import { PersonList } from "./person_list";
import { useAppStore } from "@/providers/app_store_provider";
import * as api from "@/lib/api";
import { PotentialMatchSummary } from "@/lib/api";

// Mock the next/navigation
jest.mock("next/navigation", () => ({
  useRouter: (): { push: jest.Mock } => ({
    push: jest.fn(),
  }),
}));

// Mock the app store and API
jest.mock("@/providers/app_store_provider");
jest.mock("@/lib/api");

describe("PersonList E2E", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should fetch and display potential matches with max match probabilities", async (): Promise<void> => {
    // Mock potential matches with different probabilities
    const mockPotentialMatches: PotentialMatchSummary[] = [
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

    // Mock the API response
    (api.fetchPotentialMatches as jest.Mock).mockResolvedValue(
      mockPotentialMatches,
    );

    // Create a mock implementation of fetchSummaries that uses the API
    const mockFetchSummaries = jest.fn(async () => {
      const matches = await api.fetchPotentialMatches({});
      mockState.personMatch.potentialMatchSummaries = {};

      // Add each match to the state
      for (const match of matches) {
        mockState.personMatch.potentialMatchSummaries[match.id] = match;
      }

      // Force re-render by triggering a state update
      renderComponent();
    });

    // Mock state for the component
    const mockState = {
      personMatch: {
        matchMode: true,
        potentialMatchSummaries: {} as Record<string, PotentialMatchSummary>,
        selectedPotentialMatchId: null,
        personSummaries: {},
        selectedPersonId: null,
        selectSummary: jest.fn(),
        clearSearchTerms: jest.fn(),
        dataSources: [],
        searchTerms: {},
        updateSearchTerms: jest.fn(),
        fetchSummaries: mockFetchSummaries,
      },
    };

    // Mock the app store to return our mock state
    (useAppStore as jest.Mock).mockImplementation((selector) => {
      if (typeof selector === "function") {
        return selector(mockState);
      }
    });

    // Function to render the component (used for initial render and re-renders)
    const renderComponent = (): ReturnType<typeof render> =>
      render(<PersonList />);

    // Render the component
    renderComponent();

    // Call fetchSummaries to simulate API call
    await mockFetchSummaries();

    // Verify max match probabilities are displayed after data is loaded
    await waitFor(() => {
      expect(screen.getByText("85% match")).toBeInTheDocument();
      expect(screen.getByText("92% match")).toBeInTheDocument();
    });

    // Verify the API was called
    expect(api.fetchPotentialMatches).toHaveBeenCalled();
  });
});
