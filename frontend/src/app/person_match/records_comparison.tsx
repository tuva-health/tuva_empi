import { X } from "lucide-react";

import { getCommonKeys } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { useAppStore } from "@/providers/app_store_provider";
import { PersonRecordWithMetadata } from "@/lib/stores/person_match_slice";

interface RecordCardProps {
  commonKeys: string[];
  record: PersonRecordWithMetadata;
}

const RecordCard = ({ record, commonKeys }: RecordCardProps): JSX.Element => {
  const { toggleExpandedRecord } = useAppStore((state) => state.personMatch);

  const fields = [
    { key: "first_name", label: "First Name" },
    { key: "last_name", label: "Last Name" },
    { key: "birth_date", label: "DOB" },
    { key: "social_security_number", label: "Social Security" },
    { key: "sex", label: "Sex" },
    { key: "race", label: "Race" },
    { key: "address", label: "Address" },
    { key: "city", label: "City" },
    { key: "state", label: "State" },
    { key: "data_source", label: "Data Source" },
    { key: "source_person_id", label: "Data Source ID" },
  ];

  const handleClose = (): void => {
    toggleExpandedRecord(record.id, record);
  };

  return (
    <div className="w-[280px] min-w-[280px]">
      <div className="flex flex-col gap-4 w-full pb-4 border rounded-md overflow-hidden">
        <div
          className={`flex items-center justify-between gap-4 w-full h-6 ${record.bgClassName + "/8"}`}
        >
          <div className={`w-4 h-full ${record.bgClassName}`} />

          <Button
            variant="ghost"
            className="h-auto p-1 rounded-full"
            onClick={handleClose}
          >
            <X className="w-4 h-4 text-muted-foreground" />
          </Button>
        </div>

        <div className="flex flex-col gap-2 w-full px-4">
          {fields.map((field) => (
            <div key={field.key}>
              <p className="text-xs text-muted-foreground">{field.label}</p>
              <p
                className={`min-h-5 text-sm text-primary ${commonKeys.includes(field.key) ? "bg-green-100" : ""}`}
              >
                {record[field.key] as string}
              </p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

const RecordsComparison = (): JSX.Element => {
  const {
    matchMode,
    expandedRecords,
    selectedPersonId,
    clearExpandedRecords,
    selectedPotentialMatchId,
  } = useAppStore((state) => state.personMatch);

  const records = matchMode
    ? selectedPotentialMatchId
      ? expandedRecords[selectedPotentialMatchId]
      : {}
    : selectedPersonId
      ? expandedRecords[selectedPersonId]
      : {};

  if (!records || !Object.keys(records).length)
    return <div className="hidden" />;

  const commonKeys =
    Object.keys(records).length > 1
      ? getCommonKeys(Object.values(records))
      : [];

  return (
    <div className="w-full">
      <div className="flex flex-row-reverse w-full p-3 border-b mb-4">
        <Button variant="ghost" className="h-10" onClick={clearExpandedRecords}>
          Clear Selections
        </Button>
      </div>

      <div className="flex gap-4 pb-4 overflow-x-auto [scrollbar-width:none] [-ms-overflow-style:none] [&::-webkit-scrollbar]:hidden">
        {Object.values(records).map((record) => (
          <RecordCard key={record.id} record={record} commonKeys={commonKeys} />
        ))}

        <div />
      </div>
    </div>
  );
};

export default RecordsComparison;
