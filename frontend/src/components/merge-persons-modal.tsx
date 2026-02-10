"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { useAppStore } from "@/providers/app_store_provider";
import { PersonWithMetadata } from "@/lib/stores/person_match_slice";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogDescription,
} from "@/components/ui/dialog";

interface MergePersonsModalProps {
  isOpen: boolean;
  persons: PersonWithMetadata[];
}

export const MergePersonsModal: React.FC<MergePersonsModalProps> = ({
  isOpen,
  persons,
}) => {
  const [selectedPersonId, setSelectedPersonId] = useState<string>("");
  const [isNewPersonSelected, setIsNewPersonSelected] =
    useState<boolean>(false);

  const {
    mergePersons,
    closeMergeModal,
    createNewPerson,
    selectedPotentialMatchId,
  } = useAppStore((state) => state.personMatch);

  const bgClassNames = [
    "bg-chart-2",
    "bg-chart-1",
    "bg-chart-3",
    "bg-chart-4",
    "bg-chart-5",
  ];

  const handleSave = (): void => {
    if (isNewPersonSelected) {
      createNewPerson(selectedPotentialMatchId!, true);
    } else if (selectedPersonId) {
      mergePersons(selectedPersonId);
    }
  };

  const handleCancel = (): void => {
    setSelectedPersonId("");
    setIsNewPersonSelected(false);
    closeMergeModal();
  };

  const getPersonDisplayName = (person: PersonWithMetadata): string => {
    const isNewPerson = person.id.startsWith("new-person-");

    if (isNewPerson) {
      const personIndex = parseInt(person.id.replace("new-person-", ""));

      return `New Person ${personIndex}`;
    }
    return person.id;
  };

  return (
    <Dialog open={isOpen} onOpenChange={closeMergeModal}>
      <DialogContent className="max-w-md">
        <DialogTitle>Merge Persons</DialogTitle>
        <DialogDescription className="-mt-2">
          Data records will be assigned to single person
        </DialogDescription>

        <div className="space-y-4">
          <p className="text-sm text-foreground font-medium">
            Select which person to merge records to:
          </p>

          <div className="space-y-3">
            {persons.map((person, ndx) => (
              <div key={person.id} className="flex flex-col gap-1">
                <div className="flex items-center space-x-2">
                  <input
                    type="radio"
                    id={person.id}
                    value={person.id}
                    name="person-selection"
                    checked={selectedPersonId === person.id}
                    className="h-4 w-4 accent-current text-foreground"
                    onChange={(e) => {
                      setSelectedPersonId(e.target.value);
                      setIsNewPersonSelected(false);
                    }}
                  />

                  <div
                    className={`w-3 h-3 rounded-full ${bgClassNames[ndx % 3]}`}
                  />

                  <label
                    htmlFor={person.id}
                    className="text-sm font-medium cursor-pointer flex-1"
                  >
                    {getPersonDisplayName(person)}
                  </label>
                </div>
                <p className="ml-6 text-xs text-muted-foreground">
                  {person.records.length} data records
                </p>
              </div>
            ))}

            <hr />

            <div className="flex items-center space-x-2">
              <input
                type="radio"
                id="create-new-person"
                name="person-selection"
                value="create-new-person"
                checked={isNewPersonSelected}
                className="h-4 w-4 accent-current text-foreground"
                onChange={(e) => {
                  setIsNewPersonSelected(e.target.checked);

                  if (e.target.checked) {
                    setSelectedPersonId("");
                  }
                }}
              />

              <label
                htmlFor="create-new-person"
                className="text-sm font-medium cursor-pointer flex-1"
              >
                Create New Person
              </label>
            </div>
          </div>
        </div>

        <div className="flex flex-row-reverse gap-2">
          <Button
            onClick={handleSave}
            disabled={!selectedPersonId && !isNewPersonSelected}
          >
            Save
          </Button>

          <Button variant="outline" onClick={handleCancel}>
            Cancel
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
};
