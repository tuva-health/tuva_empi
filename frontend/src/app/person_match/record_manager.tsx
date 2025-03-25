"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { useRouter } from "next/navigation";
import { useAppStore } from "@/providers/app_store_provider";
import {
  ChevronDown,
  ChevronUp,
  // TODO: Enable this when we implement the Person details feature
  // Pencil,
  GripVertical,
  LoaderCircle,
} from "lucide-react";
import { getRoute, Route } from "@/lib/routes";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  PersonWithMetadata,
  PersonRecordWithMetadata,
  PotentialMatchWithMetadata,
} from "@/lib/stores/person_match_slice";
import { useDrag, useDrop } from "@react-aria/dnd";

interface PersonRecordRowDetailProps {
  record: PersonRecordWithMetadata;
}

interface PersonRecordRowDetailField {
  label: string;
  fieldName: string;
}

const formatMatchProbability = (
  probability: number | undefined,
): string | undefined => {
  return probability !== undefined
    ? `${Math.round(probability * 100)}%`
    : undefined;
};

export const PersonRecordRowDetail: React.FC<PersonRecordRowDetailProps> = ({
  record,
}) => {
  const formattedPercentage = formatMatchProbability(
    record.highest_match_probability,
  );

  const rows: [
    PersonRecordRowDetailField,
    PersonRecordRowDetailField | null,
  ][] = [
    [
      { label: "First Name", fieldName: "first_name" },
      { label: "Last Name", fieldName: "last_name" },
    ],
    [
      { label: "Birth Date", fieldName: "birth_date" },
      { label: "Social Security Number", fieldName: "social_security_number" },
    ],
    [
      { label: "Sex", fieldName: "sex" },
      { label: "Race", fieldName: "race" },
    ],
    [
      { label: "Address", fieldName: "address" },
      { label: "City", fieldName: "city" },
    ],
    [{ label: "State", fieldName: "state" }, null],
    [
      { label: "Data Source", fieldName: "data_source" },
      { label: "Source Person ID", fieldName: "source_person_id" },
    ],
  ];

  return (
    <div className="flex flex-row gap-6">
      <div className="flex flex-col w-full h-full py-6 pl-12 gap-2">
        {rows.map((row, ndx) => (
          <div key={ndx} className="flex flex-row w-full gap-6">
            <div className="flex flex-col w-full py-1 px-2 rounded max-w-1/2">
              <p className="text-xs text-muted-foreground">{row[0].label}</p>
              <p className="text-sm">
                {record[row[0].fieldName]?.toString() ?? ""}
              </p>
            </div>
            {row[1] ? (
              <div className="flex flex-col w-full py-1 px-2 rounded max-w-1/2">
                <p className="text-xs text-muted-foreground">{row[1].label}</p>
                <p className="text-sm">
                  {record[row[1].fieldName]?.toString() ?? ""}
                </p>
              </div>
            ) : (
              <div className="flex flex-col w-full py-1 px-2 max-w-1/2"></div>
            )}
          </div>
        ))}
        {formattedPercentage && (
          <div className="flex flex-row w-full gap-6 mb-2">
            <div className="flex flex-col w-full py-1 px-2 rounded max-w-1/2">
              <p className="text-xs text-muted-foreground">Match</p>
              <p className="text-sm font-medium">{formattedPercentage}</p>
            </div>
            <div className="flex flex-col w-full py-1 px-2 rounded max-w-1/2"></div>
          </div>
        )}
      </div>
      <div className="flex flex-col w-full h-full"></div>
    </div>
  );
};

interface PersonRecordRowProps {
  record: PersonRecordWithMetadata;
  onExpand: (recordId: string, expanded: boolean) => void;
  draggable: boolean;
}

export const PersonRecordRow: React.FC<PersonRecordRowProps> = ({
  record,
  onExpand,
  draggable,
}) => {
  const formattedPercentage = formatMatchProbability(
    record.highest_match_probability,
  );

  const { dragProps } = useDrag({
    getItems() {
      return [
        {
          "tuva/personrecord": JSON.stringify(record),
        },
      ];
    },
  });

  return (
    <React.Fragment>
      <TableRow
        className={record.expanded ? "bg-accent hover:bg-accent" : ""}
        {...(draggable ? dragProps : {})}
      >
        <TableCell>
          <GripVertical />
        </TableCell>
        <TableCell>{record.last_name}</TableCell>
        <TableCell>{record.first_name}</TableCell>
        <TableCell>{record.birth_date}</TableCell>
        <TableCell>{record.city}</TableCell>
        <TableCell>{record.state}</TableCell>
        <TableCell>{formattedPercentage}</TableCell>
        <TableCell>
          <Button
            variant="ghost"
            onClick={() => onExpand(record.id, !record.expanded)}
          >
            {record.expanded ? <ChevronUp /> : <ChevronDown />}
          </Button>
        </TableCell>
      </TableRow>
      <TableRow className={record.expanded ? "" : "hidden"}>
        <TableCell colSpan={7} className="bg-white hover:bg-white">
          <PersonRecordRowDetail record={record} />
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

interface PersonRowProps {
  person: PersonWithMetadata;
  ndx: number;
  onExpandRecord: (
    personId: string,
    recordId: string,
    expanded: boolean,
  ) => void;
  onRecordDrop?: (
    personRecord: PersonRecordWithMetadata,
    toPersonId: string,
  ) => void;
  recordDraggable: boolean;
}

const PersonRow: React.FC<PersonRowProps> = ({
  person,
  ndx,
  onExpandRecord,
  onRecordDrop,
  recordDraggable,
}: PersonRowProps) => {
  const bgClassNames = [
    "bg-chart-2",
    "bg-chart-1",
    "bg-chart-3",
    "bg-chart-4",
    "bg-chart-5",
  ];
  const bgClassNamesMuted = [
    "bg-chart-2/15",
    "bg-chart-1/15",
    "bg-chart-3/15",
    "bg-chart-4/15",
    "bg-chart-5/15",
  ];
  const bgClassNamesLessMuted = [
    "bg-chart-2/25",
    "bg-chart-1/25",
    "bg-chart-3/25",
    "bg-chart-4/25",
    "bg-chart-5/25",
  ];
  const bgClassName = bgClassNames[ndx % 5];
  const bgClassNameMuted = bgClassNamesMuted[ndx % 5];
  const bgClassNameLessMuted = bgClassNamesLessMuted[ndx % 5];

  const ref = React.useRef(null);
  const { dropProps, isDropTarget } = useDrop({
    ref,
    async onDrop(e) {
      const dropItem = e.items[0];

      if (dropItem.kind === "text" && "getText" in dropItem) {
        const personRecordJSON = await dropItem.getText("tuva/personrecord");
        const personRecord = JSON.parse(personRecordJSON);

        if (personRecord && onRecordDrop) {
          onRecordDrop(personRecord, person.id);
        }
      } else {
        console.error("getText is not available on this DropItem type.");
      }
    },
  });

  const isNewPerson = person.id.startsWith("new-person-");
  const personIndex = isNewPerson
    ? parseInt(person.id.replace("new-person-", ""))
    : undefined;
  const displayText = isNewPerson ? `New Person ${personIndex}` : person.id;

  return (
    <React.Fragment>
      <TableRow
        className={isDropTarget ? bgClassNameLessMuted : "hover:bg-transparent"}
        ref={ref}
        {...dropProps}
      >
        <TableCell className={`${bgClassName}`}></TableCell>
        <TableCell className={`${bgClassNameMuted}`}>{displayText}</TableCell>
        <TableCell className={`${bgClassNameMuted}`}></TableCell>
        <TableCell className={`${bgClassNameMuted}`}></TableCell>
        <TableCell className={`${bgClassNameMuted}`}></TableCell>
        <TableCell className={`${bgClassNameMuted}`}></TableCell>
        <TableCell className={`${bgClassNameMuted}`}></TableCell>
        <TableCell className={`${bgClassNameMuted}`}>
          <div className="w-[48px] h-[36px]"></div>
          {/**
           * TODO: Enable this when we implement the Person details feature
           * <Button variant="ghost">
           *   <Pencil />
           * </Button>
           */}
        </TableCell>
      </TableRow>
      {person.records.map((record) => (
        <PersonRecordRow
          key={record.id}
          record={record}
          onExpand={onExpandRecord.bind(null, person.id)}
          draggable={recordDraggable}
        />
      ))}
    </React.Fragment>
  );
};

export const RecordManager: React.FC = () => {
  const router = useRouter();
  const matchMode = useAppStore((state) => state.personMatch.matchMode);
  const currentPotentialMatches = useAppStore(
    (state) => state.personMatch.currentPotentialMatches,
  );
  const selectedPotentialMatchId = useAppStore(
    (state) => state.personMatch.selectedPotentialMatchId,
  );
  const currentPersons = useAppStore(
    (state) => state.personMatch.currentPersons,
  );
  const selectedPersonId = useAppStore(
    (state) => state.personMatch.selectedPersonId,
  );
  const resetCurrentPotentialMatch = useAppStore(
    (state) => state.personMatch.resetCurrentPotentialMatch,
  );
  const setPersonRecordExpanded = useAppStore(
    (state) => state.personMatch.setPersonRecordExpanded,
  );
  const movePersonRecord = useAppStore(
    (state) => state.personMatch.movePersonRecord,
  );
  const matchPersonRecords = useAppStore(
    (state) => state.personMatch.matchPersonRecords,
  );
  const createNewPerson = useAppStore(
    (state) => state.personMatch.createNewPerson,
  );

  const potentialMatch =
    matchMode && selectedPotentialMatchId
      ? currentPotentialMatches[selectedPotentialMatchId]
      : undefined;
  const persons =
    !matchMode && selectedPersonId && selectedPersonId in currentPersons
      ? [currentPersons[selectedPersonId]]
      : [];
  const loading = matchMode
    ? selectedPotentialMatchId && !potentialMatch
    : selectedPersonId && persons.length === 0;

  return (
    <div className="flex flex-col w-full gap-6 pt-4 pb-6 pl-5">
      <div className="flex flex-row h-[40px] items-center justify-between">
        <h3 className="text-primary scroll-m-20 text-xl font-semibold tracking-tight">
          Record Management
        </h3>
        {matchMode && potentialMatch ? (
          <div className="flex flex-row gap-2">
            <Button
              variant="ghost"
              onClick={() => resetCurrentPotentialMatch(potentialMatch.id)}
            >
              Cancel
            </Button>
            <Button
              onClick={() => {
                matchPersonRecords(potentialMatch.id);

                const params: { matchMode?: string; id?: string } = {};

                if (matchMode) {
                  params.matchMode = "true";
                }
                router.push(getRoute(Route.personMatch, undefined, params), {
                  scroll: false,
                });
              }}
            >
              Save
            </Button>
          </div>
        ) : (
          <></>
        )}
      </div>
      {(matchMode && potentialMatch) || (!matchMode && persons.length > 0) ? (
        <div className="h-full w-full overflow-y-auto">
          <div className="border rounded">
            <Table>
              <TableHeader>
                <TableRow className="pointer-events-none">
                  <TableHead className="w-[32px]"></TableHead>
                  <TableHead>Last Name</TableHead>
                  <TableHead>First Name</TableHead>
                  <TableHead>Birth Date</TableHead>
                  <TableHead>City</TableHead>
                  <TableHead>State</TableHead>
                  {matchMode && <TableHead>Match</TableHead>}
                  <TableHead className="w-[64px]"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {Object.values(
                  matchMode
                    ? (potentialMatch as PotentialMatchWithMetadata).persons
                    : persons,
                ).map((person, ndx) => (
                  <PersonRow
                    key={person.id}
                    person={person}
                    ndx={ndx}
                    onExpandRecord={(
                      personId: string,
                      recordId: string,
                      expanded: boolean,
                    ) =>
                      setPersonRecordExpanded(
                        personId,
                        recordId,
                        expanded,
                        matchMode
                          ? (potentialMatch as PotentialMatchWithMetadata).id
                          : undefined,
                      )
                    }
                    onRecordDrop={
                      matchMode
                        ? movePersonRecord.bind(
                            null,
                            (potentialMatch as PotentialMatchWithMetadata).id,
                          )
                        : undefined
                    }
                    recordDraggable={matchMode}
                  />
                ))}
              </TableBody>
            </Table>
          </div>
          {matchMode && potentialMatch && (
            <div className="flex flex-row items-start justify-start pt-6">
              <Button
                variant="outline"
                onClick={() => createNewPerson(potentialMatch.id)}
              >
                + Create New Person
              </Button>
            </div>
          )}
        </div>
      ) : loading ? (
        <div className="flex flex-row items-center justify-center max-h-[444px] h-full w-full">
          <LoaderCircle className="animate-spin" />
        </div>
      ) : (
        <div className="flex flex-row items-center justify-center max-h-[444px] h-full w-full bg-zinc-50">
          <p className="text-sm">
            {`Select ${matchMode ? "potential matches" : "persons"} on the left to view and manage records`}
          </p>
        </div>
      )}
    </div>
  );
};
