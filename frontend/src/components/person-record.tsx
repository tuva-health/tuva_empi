import { useRef, Fragment, useEffect } from "react";

import { useDrag, useDrop } from "@react-aria/dnd";
import {
  Trash2,
  CircleCheck,
  // TODO: Enable this when we implement the Person details feature
  // Pencil,
  GripVertical,
} from "lucide-react";
import {
  TableRow,
  TableCell,
  TableHead,
  TableHeader,
} from "@/components/ui/table";
import {
  PersonWithMetadata,
  PersonRecordWithMetadata,
} from "@/lib/stores/person_match_slice";
import { formatMatchProbability } from "@/lib/utils";
import { useAppStore } from "@/providers/app_store_provider";

interface PersonRecordRowProps {
  record: PersonRecordWithMetadata;
  draggable: boolean;
  bgClassName: string;
}

const PersonRecordRow: React.FC<PersonRecordRowProps> = ({
  record,
  bgClassName,
  draggable,
}) => {
  const {
    matchMode,
    expandedRecords,
    selectedPersonId,
    selectedPotentialMatchId,
    toggleExpandedRecord,
  } = useAppStore((state) => state.personMatch);

  const rowRef = useRef<HTMLTableRowElement>(null);
  const handleRef = useRef<HTMLTableCellElement>(null);

  const formattedPercentage = formatMatchProbability(
    record.highest_match_probability,
    true,
  );

  const isExpanded = matchMode
    ? selectedPotentialMatchId &&
      selectedPotentialMatchId in expandedRecords &&
      record.id in expandedRecords[selectedPotentialMatchId]
    : selectedPersonId &&
      selectedPersonId in expandedRecords &&
      record.id in expandedRecords[selectedPersonId];

  const formatDate = (date: string | undefined): string => {
    if (!date) return "";

    return new Date(
      Number(date.slice(0, 4)),
      Number(date.slice(4, 6)) - 1,
      Number(date.slice(6, 8)),
    ).toLocaleDateString();
  };

  const { dragProps, isDragging } = useDrag({
    getItems: () => [{ "tuva/personrecord": JSON.stringify(record) }],
  });

  const handleToggleRecord = (): void => {
    toggleExpandedRecord(record.id, { ...record, bgClassName });
  };

  useEffect(() => {
    if (!draggable) return;

    const rowEl = rowRef.current;
    const handleEl = handleRef.current;

    if (!rowEl || !handleEl) return;

    const onDragStart = (e: DragEvent): void => {
      try {
        if (rowEl && e.dataTransfer) {
          e.dataTransfer.setDragImage(rowEl, 0, 0);
        }
      } catch (error) {
        console.warn("Error setting drag image:", error);
      }
    };

    handleEl.addEventListener("dragstart", onDragStart);

    return (): void => {
      handleEl.removeEventListener("dragstart", onDragStart);
    };
  }, [draggable]);

  return (
    <TableRow
      ref={rowRef}
      onClick={handleToggleRecord}
      className={`h-9 cursor-pointer hover:bg-accent transition-all ${
        isDragging ? "opacity-50 bg-gray-100" : ""
      } ${isExpanded ? "bg-accent" : ""}`}
    >
      <TableCell
        ref={handleRef}
        draggable={draggable}
        className="p-0 cursor-grab"
        {...(draggable ? dragProps : {})}
      >
        <GripVertical className="mx-auto" />
      </TableCell>
      <TableCell>{record.last_name}</TableCell>
      <TableCell>{record.first_name}</TableCell>
      <TableCell>{formatDate(record.birth_date)}</TableCell>
      <TableCell>{record.city}</TableCell>
      <TableCell>{record.state}</TableCell>
      {formattedPercentage && <TableCell>{formattedPercentage}</TableCell>}
      <TableCell>
        {record.matched_or_reviewed ? (
          <CircleCheck className="w-4 h-4" data-testid="check" />
        ) : (
          <span className="font-normal text-foreground">New</span>
        )}
      </TableCell>
    </TableRow>
  );
};

interface PersonRowProps {
  person: PersonWithMetadata;
  ndx: number;
  matchMode: boolean;
  personsCount: number;
  onRecordDrop?: (
    personRecord: PersonRecordWithMetadata,
    toPersonId: string,
  ) => void;
  removePerson: (id: string) => void;
}

export const PersonRow: React.FC<PersonRowProps> = ({
  person,
  ndx,
  matchMode,
  personsCount,
  onRecordDrop,
  removePerson,
}: PersonRowProps) => {
  const ref = useRef(null);

  const bgClassNames = [
    "bg-chart-2",
    "bg-chart-1",
    "bg-chart-3",
    "bg-chart-4",
    "bg-chart-5",
  ];
  const bgClassNamesMuted = [
    "bg-chart-2/8",
    "bg-chart-1/8",
    "bg-chart-3/8",
    "bg-chart-4/8",
    "bg-chart-5/8",
  ];
  const bgClassNamesLessMuted = [
    "bg-chart-2/25",
    "bg-chart-1/25",
    "bg-chart-3/25",
    "bg-chart-4/25",
    "bg-chart-5/25",
  ];
  const bgClassName = bgClassNames[ndx % 3];
  const bgClassNameMuted = bgClassNamesMuted[ndx % 3];
  const bgClassNameLessMuted = bgClassNamesLessMuted[ndx % 3];

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
  const isRemoveable = !person.records.length && personsCount > 1;

  const handleRemovePerson = (): void => {
    removePerson(person.id);
  };

  return (
    <Fragment>
      <TableRow
        className={`h-9 text-muted-text ${isDropTarget ? bgClassNameLessMuted : "hover:bg-transparent"}`}
        ref={ref}
        {...dropProps}
      >
        <TableCell className={`${bgClassName}`}></TableCell>
        <TableCell
          colSpan={!isRemoveable ? 7 : 6}
          className={`${bgClassNameMuted}`}
        >
          {displayText}
        </TableCell>

        {isRemoveable && (
          <TableCell className={`${bgClassNameMuted}`}>
            <Trash2
              className="w-4 h-4 ml-auto mr-4 cursor-pointer"
              onClick={handleRemovePerson}
            />
          </TableCell>
        )}

        {/* <TableCell className={`${bgClassNameMuted}`}>
          <div className="w-[48px] h-[36px]"></div>

          TODO: Enable this when we implement the Person details feature
          <Button variant="ghost">
            <Pencil />
          </Button>

        </TableCell> */}
      </TableRow>

      {person.records.map((record) => (
        <PersonRecordRow
          key={record.id}
          record={record}
          draggable={matchMode}
          bgClassName={bgClassName}
        />
      ))}
    </Fragment>
  );
};

interface TableHeaderProps {
  matchMode: boolean;
}

export const RecordTableHeader: React.FC<TableHeaderProps> = ({
  matchMode,
}) => (
  <TableHeader>
    <TableRow className="h-12 pointer-events-none select-none">
      <TableHead className="w-8"></TableHead>
      <TableHead>Last Name</TableHead>
      <TableHead>First Name</TableHead>
      <TableHead>DOB</TableHead>
      <TableHead>City</TableHead>
      <TableHead>State</TableHead>
      {matchMode && <TableHead>Match</TableHead>}
      <TableHead>Status</TableHead>
    </TableRow>
  </TableHeader>
);
