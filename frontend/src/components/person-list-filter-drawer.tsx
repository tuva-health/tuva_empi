import React from "react";
import { Button } from "@/components/ui/button";
import { useAppStore } from "@/providers/app_store_provider";
import { Search, X } from "lucide-react";
import {
  Drawer,
  DrawerClose,
  DrawerContent,
  DrawerFooter,
  DrawerHeader,
  DrawerTitle,
  DrawerDescription,
  DrawerTrigger,
} from "@/components/ui/drawer";
import { Combobox } from "@/components/combobox";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";

const PersonListFilterDrawer: React.FC = () => {
  const {
    dataSources,
    searchTerms,
    fetchSummaries,
    clearSearchTerms,
    updateSearchTerms,
  } = useAppStore((state) => state.personMatch);

  const dataSourceOptions = dataSources?.map((dataSource) => ({
    value: dataSource.name,
    label: dataSource.name,
  }));

  return (
    <Drawer direction="left" handleOnly={true}>
      <DrawerTrigger asChild>
        <Button variant="ghost" size="sm" className="w-10 h-10 border">
          <Search className="w-4 h-4" />
        </Button>
      </DrawerTrigger>
      <DrawerContent className="h-full max-w-sm border-r justify-between p-6">
        <div className="h-full w-full flex flex-col relative gap-4">
          <DrawerClose>
            <X className="absolute h-4 w-4 top-0 right-0" />
          </DrawerClose>
          <DrawerHeader>
            <DrawerTitle>Filter Persons</DrawerTitle>
            <DrawerDescription className="text-xs">
              Narrow down persons by using the filters below.
            </DrawerDescription>
          </DrawerHeader>
          <div className="w-full flex flex-col gap-2">
            <Label
              htmlFor="data-sources-filter"
              className="text-sm font-medium"
            >
              Data Sources
            </Label>
            <Combobox
              id="data-sources-filter"
              items={dataSourceOptions}
              placeholder="All data sources"
              initialValue={searchTerms.data_source}
              onChange={(value: string) =>
                updateSearchTerms("data_source", value)
              }
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="first-name-filter" className="text-sm font-medium">
              First Name
            </Label>
            <Input
              id="first-name-filter"
              value={searchTerms.first_name ?? ""}
              onChange={(e) => updateSearchTerms("first_name", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="last-name-filter" className="text-sm font-medium">
              Last Name
            </Label>
            <Input
              id="last-name-filter"
              value={searchTerms.last_name ?? ""}
              onChange={(e) => updateSearchTerms("last_name", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="birth-date-filter" className="text-sm font-medium">
              Date of Birth
            </Label>
            <Input
              id="birth-date-filter"
              value={searchTerms.birth_date ?? ""}
              onChange={(e) => updateSearchTerms("birth_date", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="pid-filter" className="text-sm font-medium">
              Person ID
            </Label>
            <Input
              id="pid-filter"
              value={searchTerms.person_id ?? ""}
              onChange={(e) => updateSearchTerms("person_id", e.target.value)}
            />
          </div>
          <div className="w-full flex flex-col gap-2">
            <Label htmlFor="spid-filter" className="text-sm font-medium">
              Source Person ID
            </Label>
            <Input
              id="spid-filter"
              value={searchTerms.source_person_id ?? ""}
              onChange={(e) =>
                updateSearchTerms("source_person_id", e.target.value)
              }
            />
          </div>

          {/* TODO: Add Minimum Probability & Maximum Probability filters */}
        </div>
        <DrawerFooter className="flex flex-row justify-between">
          <Button variant="ghost" onClick={clearSearchTerms}>
            Clear Filters
          </Button>
          <div className="flex flex-row gap-2">
            <DrawerClose className="inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 hover:bg-accent hover:text-accent-foreground h-9 px-4 py-2">
              Cancel
            </DrawerClose>
            <DrawerClose
              onClick={fetchSummaries}
              className="inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 bg-primary text-primary-foreground shadow hover:bg-primary/90 h-9 px-4 py-2"
            >
              Filter
            </DrawerClose>
          </div>
        </DrawerFooter>
      </DrawerContent>
    </Drawer>
  );
};

export default PersonListFilterDrawer;
