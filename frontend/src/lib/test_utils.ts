import { AppStore } from "./stores/types";

export const createMockStore = (overrides: Partial<AppStore> = {}): AppStore => {
  const defaultStore: AppStore = {
    personMatch: {
      dataSources: [],
      matchMode: false,
      searchTerms: {},
      potentialMatchSummaries: {},
      potentialMatches: {},
      currentPotentialMatches: {},
      selectedPotentialMatchId: null,
      personSummaries: {},
      persons: {},
      currentPersons: {},
      selectedPersonId: null,
      createNewPerson: jest.fn(),
      fetchDataSources: jest.fn().mockResolvedValue(undefined),
      setMatchMode: jest.fn(),
      updateSearchTerms: jest.fn(),
      clearSearchTerms: jest.fn(),
      fetchSummaries: jest.fn().mockResolvedValue(undefined),
      selectSummary: jest.fn(),
      fetchPotentialMatch: jest.fn().mockResolvedValue(undefined),
      resetCurrentPotentialMatch: jest.fn(),
      fetchPerson: jest.fn().mockResolvedValue(undefined),
      resetCurrentPerson: jest.fn(),
      movePersonRecord: jest.fn(),
      matchPersonRecords: jest.fn().mockResolvedValue(undefined),
      setPersonRecordExpanded: jest.fn(),
    },
  };

  return {
    ...defaultStore,
    ...overrides,
  };
};
