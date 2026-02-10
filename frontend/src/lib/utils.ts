import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

import { PotentialMatch } from "@/lib/api";
import { PotentialMatchWithMetadata } from "@/lib/stores/person_match_slice";

export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}

// Format match probability for display
export const formatMatchProbability = (
  probability: number | undefined,
  short?: boolean,
): string | undefined => {
  if (!probability) {
    return undefined;
  }

  const displayProbability = Math.round(probability * 100);

  return `${displayProbability}%${short ? "" : " match"}`;
};

export const isPotentialMatchUpdated = (
  potentialMatch: PotentialMatchWithMetadata | undefined,
  initialPotentialMatch: PotentialMatch | undefined,
): boolean => {
  return (
    !!potentialMatch &&
    !!initialPotentialMatch &&
    (Object.keys(potentialMatch?.persons).length !==
      initialPotentialMatch?.persons?.length ||
      Object.keys(potentialMatch?.persons).some((key) => {
        const records = potentialMatch.persons[key]?.records ?? [];
        const initialRecords =
          initialPotentialMatch?.persons?.find((p) => p.id === key)?.records ??
          [];

        if (records.length !== initialRecords.length) return true;

        const ids = records.map((r) => r.id).sort();
        const initialIds = initialRecords.map((r) => r.id).sort();

        return ids.join(",") !== initialIds.join(",");
      }))
  );
};

export const getCommonKeys = (arr: Record<string, any>[]) => {
  if (!arr.length) return [];

  return Object.keys(arr[0]).filter((key) => {
    const firstValue = arr[0][key];
    return arr.every((obj) => obj[key] === firstValue);
  });
};
