import { fetchPotentialMatches } from "./api";

// Mock the global fetch function
const mockFetch = jest.fn();
global.fetch = mockFetch;
global.console.info = jest.fn();
global.console.error = jest.fn();

describe("fetchPotentialMatches", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Set up environment variable
    process.env.NEXT_PUBLIC_API_BASE_URL = "http://api.example.com/";
  });

  afterEach(() => {
    delete process.env.NEXT_PUBLIC_API_BASE_URL;
  });

  it("should fetch potential matches from the API", async () => {
    // Mock API response data
    const mockPotentialMatches = [
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

    // Mock the fetch response
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ potential_matches: mockPotentialMatches }),
    });

    // Call the function
    const result = await fetchPotentialMatches();

    // Verify fetch was called with the correct URL
    expect(mockFetch).toHaveBeenCalledWith(
      "http://api.example.com/potential-matches?",
    );

    // Verify the correct data was returned
    expect(result).toEqual(mockPotentialMatches);
  });

  it("should include search terms in the request URL", async () => {
    // Mock API response data
    const mockPotentialMatches = [
      {
        id: "123",
        first_name: "John",
        last_name: "Doe",
        data_sources: ["source1"],
        max_match_probability: 0.85,
      },
    ];

    // Mock the fetch response
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ potential_matches: mockPotentialMatches }),
    });

    // Call the function with search terms
    const searchTerms = {
      first_name: "John",
      last_name: "Doe",
      birth_date: "1980-01-01",
    };
    await fetchPotentialMatches(searchTerms);

    // Verify fetch was called with the correct URL including search parameters
    expect(mockFetch).toHaveBeenCalledWith(
      "http://api.example.com/potential-matches?first_name=John&last_name=Doe&birth_date=1980-01-01",
    );
  });

  it("should handle API errors gracefully", async () => {
    // Mock a fetch error
    mockFetch.mockRejectedValueOnce(new Error("Network error"));

    // Call the function
    const result = await fetchPotentialMatches();

    // Verify fetch was called
    expect(mockFetch).toHaveBeenCalled();

    // Verify error is logged
    expect(console.error).toHaveBeenCalled();

    // Verify an empty array is returned on error
    expect(result).toEqual([]);
  });
});
