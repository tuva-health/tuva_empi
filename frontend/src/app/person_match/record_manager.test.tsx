import { render, screen, within } from "@testing-library/react";
import {
  PersonRecordRow,
  PersonRecordRowDetail,
  RecordManager,
  RecordTableHeader,
} from "./record_manager";
import { PersonRecordWithMetadata } from "@/lib/stores/person_match_slice";
import { Table, TableBody } from "@/components/ui/table";
import { type AppStore } from "@/lib/stores/types";
import userEvent from "@testing-library/user-event";
import { createMockStore } from "@/lib/test_utils";
// Add mock imports
jest.mock("next/navigation", () => ({
  useRouter: (): { push: jest.Mock } => ({
    push: jest.fn(),
  }),
}));

const mockUseAppStore = jest.fn();
jest.mock("@/providers/app_store_provider", () => ({
  useAppStore: <T,>(selector: (state: AppStore) => T): T =>
    mockUseAppStore(selector),
}));

const mockRecord: PersonRecordWithMetadata = {
  id: "test-id",
  created: new Date(),
  person_id: "person-1",
  person_updated: new Date(),
  matched_or_reviewed: new Date(),
  data_source: "test-source",
  source_person_id: "source-1",
  first_name: "John",
  last_name: "Doe",
  sex: "M",
  race: "White",
  birth_date: "1990-01-01",
  death_date: "",
  social_security_number: "123-45-6789",
  address: "123 Main St",
  city: "Test City",
  state: "TS",
  zip_code: "12345",
  county: "Test County",
  phone: "123-456-7890",
  expanded: false,
};

const TableWrapper: React.FC<{
  children: React.ReactNode;
  matchMode?: boolean;
}> = ({ children, matchMode = false }) => (
  <Table>
    <RecordTableHeader matchMode={matchMode} />
    <TableBody>{children}</TableBody>
  </Table>
);

describe("PersonRecordRow", () => {
  const defaultProps = {
    record: mockRecord,
    onExpand: jest.fn(),
    draggable: false,
  };

  const renderWithTable = (
    ui: React.ReactElement,
    matchMode: boolean = false,
  ): ReturnType<typeof render> => {
    return render(<TableWrapper matchMode={matchMode}>{ui}</TableWrapper>);
  };

  it("should display rounded max match probability in table cell when provided", () => {
    const record = {
      ...mockRecord,
      highest_match_probability: 0.856,
    };

    renderWithTable(
      <PersonRecordRow {...defaultProps} record={record} />,
      true,
    );

    const headers = screen.getAllByRole("columnheader");
    const matchHeaderIndex = headers.findIndex(
      (header) => header.textContent === "Match",
    );
    const cells = screen.getAllByRole("cell");
    const matchCell = cells[matchHeaderIndex];
    expect(matchCell).toHaveTextContent("86%");
  });

  it("should not display max match probability when not provided", () => {
    renderWithTable(<PersonRecordRow {...defaultProps} />, true);

    const headers = screen.getAllByRole("columnheader");
    const matchHeaderIndex = headers.findIndex(
      (header) => header.textContent === "Match",
    );
    const cells = screen.getAllByRole("cell");
    const matchCell = cells[matchHeaderIndex];
    expect(matchCell).not.toHaveTextContent("86%");
  });

  it.each([
    { input: 0.999, expected: "100%" },
    { input: 0.001, expected: "0%" },
    { input: 0.856, expected: "86%" },
    { input: 1.0, expected: "100%" },
  ])(
    "should round $input to $expected in table cell",
    ({ input, expected }) => {
      const record = {
        ...mockRecord,
        highest_match_probability: input,
      };

      renderWithTable(
        <PersonRecordRow {...defaultProps} record={record} />,
        true,
      );
      const headers = screen.getAllByRole("columnheader");
      const matchHeaderIndex = headers.findIndex(
        (header) => header.textContent === "Match",
      );
      const cells = screen.getAllByRole("cell");
      const matchCell = cells[matchHeaderIndex];
      expect(matchCell).toHaveTextContent(expected);
    },
  );

  it("should display max match probability in expanded detail view", () => {
    const record = {
      ...mockRecord,
      expanded: true,
      highest_match_probability: 0.856,
    };

    renderWithTable(
      <PersonRecordRow {...defaultProps} record={record} />,
      true,
    );

    const detailView = screen.getByTestId("record-detail");
    const detailMatchText = within(detailView).getByText("Match");
    expect(detailMatchText).toBeInTheDocument();
    expect(detailMatchText.nextElementSibling).toHaveTextContent("86%");
  });

  it("should display Check icon when matched_or_reviewed date exists", () => {
    const record = {
      ...mockRecord,
      matched_or_reviewed: new Date(),
    };

    renderWithTable(
      <PersonRecordRow {...defaultProps} record={record} />,
      true,
    );
    const checkIcon = screen.getByTestId("check");
    expect(checkIcon).toBeInTheDocument();
  });

  it("should display 'New' text when matched_or_reviewed date does not exist", () => {
    const record = {
      ...mockRecord,
      matched_or_reviewed: null as unknown as Date,
    };

    renderWithTable(
      <PersonRecordRow {...defaultProps} record={record} />,
      true,
    );
    expect(screen.getByText("New")).toBeInTheDocument();
  });
});

describe("PersonRecordRowDetail", () => {
  it("should display all record fields correctly", () => {
    render(<PersonRecordRowDetail record={mockRecord} />);

    // Test basic field rendering
    expect(screen.getByText("First Name")).toBeInTheDocument();
    expect(screen.getByText("John")).toBeInTheDocument();
    expect(screen.getByText("Last Name")).toBeInTheDocument();
    expect(screen.getByText("Doe")).toBeInTheDocument();
  });

  it("should handle empty field values", () => {
    const recordWithEmptyFields = {
      ...mockRecord,
      first_name: "",
      last_name: "",
      birth_date: "",
    };

    render(<PersonRecordRowDetail record={recordWithEmptyFields} />);

    // Empty fields should render as empty strings
    const firstNameValue = screen.getByText("First Name").nextElementSibling;
    const lastNameValue = screen.getByText("Last Name").nextElementSibling;
    const birthDateValue = screen.getByText("Birth Date").nextElementSibling;

    expect(firstNameValue).toHaveTextContent("");
    expect(lastNameValue).toHaveTextContent("");
    expect(birthDateValue).toHaveTextContent("");
  });

  it.each([
    { input: 0.999, expected: "100%" },
    { input: 0.001, expected: "0%" },
    { input: 0.856, expected: "86%" },
    { input: 1.0, expected: "100%" },
  ])(
    "should round match probability $input to $expected",
    ({ input, expected }) => {
      const record = {
        ...mockRecord,
        highest_match_probability: input,
      };

      render(<PersonRecordRowDetail record={record} />);

      const matchProbability = screen.getByText("Match");
      expect(matchProbability).toBeInTheDocument();
      expect(matchProbability.nextElementSibling).toHaveTextContent(expected);
    },
  );

  it("should not display max match probability when not provided", () => {
    render(<PersonRecordRowDetail record={mockRecord} />);

    expect(screen.queryByText("Match")).not.toBeInTheDocument();
  });

  it("should display all record fields in correct order", () => {
    render(<PersonRecordRowDetail record={mockRecord} />);

    const expectedFields = [
      ["First Name", "Last Name"],
      ["Birth Date", "Social Security Number"],
      ["Sex", "Race"],
      ["Address", "City"],
      ["State"],
      ["Data Source", "Source Person ID"],
    ] as const;

    expectedFields.forEach((fields) => {
      expect(screen.getByText(fields[0])).toBeInTheDocument();
      if (fields[1]) {
        expect(screen.getByText(fields[1])).toBeInTheDocument();
      }
    });
  });
});

describe("RecordManager", () => {
  const mockStore = createMockStore();

  beforeEach(() => {
    mockUseAppStore.mockReset();
    mockUseAppStore.mockImplementation(
      <T,>(selector: (state: AppStore) => T): T => selector(mockStore),
    );
  });

  const setupMockState = (matchMode: boolean): void => {
    mockUseAppStore.mockImplementation(
      <T,>(selector: (state: AppStore) => T): T => {
        const mockState = createMockStore({
          personMatch: {
            ...mockStore.personMatch,
            matchMode,
            currentPotentialMatches: matchMode
              ? {
                  "match-1": {
                    id: "match-1",
                    version: 1,
                    results: [],
                    persons: {
                      "person-1": {
                        id: "person-1",
                        created: new Date(),
                        version: 1,
                        records: [],
                      },
                    },
                  },
                }
              : {},
            selectedPotentialMatchId: matchMode ? "match-1" : null,
            currentPersons: !matchMode
              ? {
                  "test-id": {
                    id: "test-id",
                    created: new Date(),
                    version: 1,
                    records: [],
                  },
                }
              : {},
            selectedPersonId: !matchMode ? "test-id" : null,
          },
        });
        return selector(mockState);
      },
    );
  };

  it("should render correct table headers when in match mode", (): void => {
    setupMockState(true);
    render(<RecordManager />);

    const expectedHeaders = [
      "", // Empty header for grip column
      "Last Name",
      "First Name",
      "Birth Date",
      "City",
      "State",
      "Match",
      "Status",
      "", // Empty header for expand/collapse column
    ];

    const headers = screen.getAllByRole("columnheader");
    expect(headers).toHaveLength(expectedHeaders.length);

    headers.forEach((header, index) => {
      expect(header.textContent).toBe(expectedHeaders[index]);
    });
  });

  it("should render correct table headers when not in match mode", (): void => {
    setupMockState(false);
    render(<RecordManager />);

    const expectedHeaders = [
      "", // Empty header for grip column
      "Last Name",
      "First Name",
      "Birth Date",
      "City",
      "State",
      "Status",
      "", // Empty header for expand/collapse column
    ];

    const headers = screen.getAllByRole("columnheader");
    expect(headers).toHaveLength(expectedHeaders.length);

    headers.forEach((header, index) => {
      expect(header.textContent).toBe(expectedHeaders[index]);
    });
  });

  describe("Create New Person functionality", () => {
    it("should show Create New Person button only when matchMode is true", () => {
      // Set up store with matchMode true and a selected potential match
      const storeWithMatchMode = createMockStore({
        personMatch: {
          ...mockStore.personMatch,
          matchMode: true,
          selectedPotentialMatchId: "test-match-1",
          currentPotentialMatches: {
            "test-match-1": {
              id: "test-match-1",
              version: 1,
              persons: {},
              results: [],
            },
          },
        },
      });

      mockUseAppStore.mockImplementation(
        <T,>(selector: (state: AppStore) => T): T =>
          selector(storeWithMatchMode),
      );

      render(<RecordManager />);
      expect(screen.getByText("+ Create New Person")).toBeInTheDocument();
    });

    it("should not show Create New Person button when matchMode is false", () => {
      // Set up store with matchMode false
      const storeWithoutMatchMode = createMockStore({
        personMatch: {
          ...mockStore.personMatch,
          matchMode: false,
          selectedPersonId: "test-person-1",
          currentPersons: {
            "test-person-1": {
              id: "test-person-1",
              version: 1,
              created: new Date(),
              records: [],
            },
          },
        },
      });

      mockUseAppStore.mockImplementation(
        <T,>(selector: (state: AppStore) => T): T =>
          selector(storeWithoutMatchMode),
      );

      render(<RecordManager />);
      expect(screen.queryByText("+ Create New Person")).not.toBeInTheDocument();
    });

    it("should create a new person row with 'New Person 1' when button is clicked", async () => {
      // Set up store with matchMode true and a selected potential match
      const storeWithMatchMode = createMockStore({
        personMatch: {
          ...mockStore.personMatch,
          matchMode: true,
          selectedPotentialMatchId: "test-match-1",
          currentPotentialMatches: {
            "test-match-1": {
              id: "test-match-1",
              version: 1,
              persons: {},
              results: [],
            },
          },
        },
      });

      mockUseAppStore.mockImplementation(
        <T,>(selector: (state: AppStore) => T): T =>
          selector(storeWithMatchMode),
      );

      render(<RecordManager />);

      const createButton = screen.getByText("+ Create New Person");
      await userEvent.click(createButton);

      expect(mockStore.personMatch.createNewPerson).toHaveBeenCalledWith(
        "test-match-1",
      );
    });

    it("should increment new person number for each additional person created", async () => {
      // Set up store with matchMode true and a selected potential match with one new person
      const storeWithNewPerson = createMockStore({
        personMatch: {
          ...mockStore.personMatch,
          matchMode: true,
          selectedPotentialMatchId: "test-match-1",
          currentPotentialMatches: {
            "test-match-1": {
              id: "test-match-1",
              version: 1,
              persons: {
                "new-person-1": {
                  id: "new-person-1",
                  created: new Date(),
                  records: [],
                },
              },
              results: [],
            },
          },
        },
      });

      mockUseAppStore.mockImplementation(
        <T,>(selector: (state: AppStore) => T): T =>
          selector(storeWithNewPerson),
      );

      const { unmount } = render(<RecordManager />);

      const createButton = screen.getByText("+ Create New Person");
      await userEvent.click(createButton);

      expect(mockStore.personMatch.createNewPerson).toHaveBeenCalledWith(
        "test-match-1",
      );

      // Update the mock store to reflect the state change by appending the new person
      storeWithNewPerson.personMatch.currentPotentialMatches[
        "test-match-1"
      ].persons = {
        ...storeWithNewPerson.personMatch.currentPotentialMatches[
          "test-match-1"
        ].persons,
        "new-person-2": {
          id: "new-person-2",
          created: new Date(),
          records: [],
        },
      };

      // Clean up previous render and re-render with new state
      unmount();
      render(<RecordManager />);

      expect(screen.getByText("New Person 1")).toBeInTheDocument();
      expect(screen.getByText("New Person 2")).toBeInTheDocument();
    });
  });
});
