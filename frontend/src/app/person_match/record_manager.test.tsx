import { render, screen } from "@testing-library/react";
import {
  PersonRecordRow,
  PersonRecordRowDetail,
  RecordManager,
} from "./record_manager";
import { PersonRecordWithMetadata } from "@/lib/stores/person_match_slice";
import { Table, TableBody } from "@/components/ui/table";
import { type AppStore } from "@/lib/stores/types";

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

const TableWrapper: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => (
  <Table>
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
  ): ReturnType<typeof render> => {
    return render(<TableWrapper>{ui}</TableWrapper>);
  };

  it("should display rounded max match probability in table cell when provided", () => {
    const record = {
      ...mockRecord,
      highest_match_probability: 0.856,
    };

    renderWithTable(<PersonRecordRow {...defaultProps} record={record} />);

    const cells = screen.getAllByRole("cell");
    const matchCell = cells[6]; // Match probability is in the 7th cell
    expect(matchCell).toHaveTextContent("86%");
  });

  it("should not display max match probability when not provided", () => {
    renderWithTable(<PersonRecordRow {...defaultProps} />);

    const cells = screen.getAllByRole("cell");
    const matchCell = cells[6]; // Match probability is in the 7th cell
    expect(matchCell).toBeEmptyDOMElement();
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

      renderWithTable(<PersonRecordRow {...defaultProps} record={record} />);
      const cells = screen.getAllByRole("cell");
      const matchCell = cells[6]; // Match probability is in the 7th cell
      expect(matchCell).toHaveTextContent(expected);
    },
  );

  it("should display max match probability in expanded detail view", () => {
    const record = {
      ...mockRecord,
      expanded: true,
      highest_match_probability: 0.856,
    };

    renderWithTable(<PersonRecordRow {...defaultProps} record={record} />);

    const detailMatchText = screen.getByText("Match");
    expect(detailMatchText).toBeInTheDocument();
    expect(detailMatchText.nextElementSibling).toHaveTextContent("86%");
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
  const setupMockState = (matchMode: boolean): void => {
    mockUseAppStore.mockImplementation(
      <T,>(selector: (state: AppStore) => T): T => {
        const mockState: AppStore = {
          personMatch: {
            dataSources: [],
            matchMode,
            searchTerms: {},
            potentialMatchSummaries: {},
            potentialMatches: {},
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
            personSummaries: {},
            persons: {},
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
      "", // Empty header for expand/collapse column
    ];

    const headers = screen.getAllByRole("columnheader");
    expect(headers).toHaveLength(expectedHeaders.length);

    headers.forEach((header, index) => {
      expect(header.textContent).toBe(expectedHeaders[index]);
    });
  });
});
