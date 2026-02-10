import { useRouter } from "next/navigation";

import { getRoute, Route } from "@/lib/routes";
import { Switch } from "@/components/ui/switch";
import { useAppStore } from "@/providers/app_store_provider";

interface MatchModeSwitchProps {
  isSidebarOpen: boolean;
}

const MatchModeSwitch: React.FC<MatchModeSwitchProps> = ({ isSidebarOpen }) => {
  const router = useRouter();
  const {
    matchMode,
    setMatchMode,
    selectedPersonId,
    selectedPotentialMatchId,
  } = useAppStore((state) => state.personMatch);

  const handleToggle = (checked: boolean): void => {
    setMatchMode(checked);

    const params: { matchMode?: string; id?: string } = {};

    if (checked) {
      params.matchMode = "true";

      if (selectedPotentialMatchId) {
        params.id = selectedPotentialMatchId;
      }
    } else {
      if (selectedPersonId) {
        params.id = selectedPersonId;
      }
    }

    router.push(getRoute(Route.personMatch, undefined, params), {
      scroll: false,
    });
  };

  return (
    <div
      className={`flex items-center justify-center gap-2 h-10 px-3 rounded-full cursor-pointer select-none ${isSidebarOpen ? "flex-1 bg-light-blue" : "w-10"}`}
      onClick={() => handleToggle(!matchMode)}
    >
      <Switch id="match-mode" checked={matchMode ?? false} />

      {isSidebarOpen && <span className="text-sm">Match Mode</span>}
    </div>
  );
};

export default MatchModeSwitch;
