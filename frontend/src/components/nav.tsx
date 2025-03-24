"use client";

import React from "react";
import Link from "next/link";
import Image from "next/image";
import Logo from "@/images/tuva_banner.svg";
import { Menu as MenuIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

interface MenuProps {
  items: { label: string; path: string }[];
}

const Menu: React.FC<MenuProps> = ({ items }: MenuProps) => {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" className="p-[10px]">
          <MenuIcon className="!h-6 !w-6" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent>
        <DropdownMenuGroup>
          {items.map((item) => (
            <DropdownMenuItem key={item.label}>{item.label}</DropdownMenuItem>
          ))}
        </DropdownMenuGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
};

interface NavProps {
  homePath: string;
  menuItems: { label: string; path: string }[];
  selectedTab: string;
}

export enum Tab {
  personMatch = "Person Match",
}

const Nav: React.FC<NavProps> = ({ homePath, menuItems, selectedTab }) => {
  return (
    <div className="h-[60px] min-h-[60px] w-full px-[24px] bg-white border-b-[1px] border-muted-foreground flex flex-row justify-between items-center">
      <div className="flex items-center h-full w-full gap-[60px]">
        <Link href={homePath}>
          <Image src={Logo} alt="Icon" className="w-[190px] h-auto" />
        </Link>
        <ul className="flex flex-row h-full items-center">
          {Object.values(Tab).map((tab) => (
            <li key={tab} className="h-full">
              {tab === selectedTab ? (
                <Button
                  variant="link"
                  disabled={true}
                  className="h-full text-black border-b-[5px] border-black disabled:opacity-100 rounded-none"
                >
                  Person Match
                </Button>
              ) : (
                <Button variant="link" className="h-full rounded-none">
                  Person Match
                </Button>
              )}
            </li>
          ))}
        </ul>
      </div>
      <Menu items={menuItems} />
    </div>
  );
};

export default Nav;
