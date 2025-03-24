import Nav from "@/components/nav";
import { Route, getRoute } from "@/lib/routes";

const navMenuItems = [{ label: "Log out", path: getRoute(Route.home) }];

interface NavBarProps {
  selectedTab: string;
}

const NavBar: React.FC<NavBarProps> = ({ selectedTab }: NavBarProps) => {
  const homePath = getRoute(Route.home);

  return (
    <Nav
      homePath={homePath}
      menuItems={navMenuItems}
      selectedTab={selectedTab}
    />
  );
};

export default NavBar;
