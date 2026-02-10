"use client";

import React, { useState } from "react";
import Link from "next/link";
import Image from "next/image";
import Logo from "@/images/tuva_banner.svg";
import {
  Menu as MenuIcon,
  CircleUser as UserIcon,
  ChevronDown as ArrowDownIcon,
} from "lucide-react";
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
  customIcon?: (isOpen: boolean) => React.ReactNode;
}

const Menu: React.FC<MenuProps> = ({ items, customIcon }: MenuProps) => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <DropdownMenu onOpenChange={setIsOpen}>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          className="p-[10px] focus-visible:ring-0 focus-visible:ring-offset-0"
        >
          {!!customIcon ? (
            customIcon(isOpen)
          ) : (
            <MenuIcon className="!h-6 !w-6" />
          )}
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
              <Button
                variant="link"
                disabled={tab === selectedTab}
                className={`h-full rounded-none hover:no-underline ${tab === selectedTab ? "text-black border-b-[5px] border-black disabled:opacity-100" : ""}`}
              >
                {tab}
              </Button>
            </li>
          ))}
        </ul>
      </div>

      <Menu
        items={menuItems}
        customIcon={(isOpen) => (
          <>
            <UserIcon className="!w-6 !h-6" />
            <ArrowDownIcon
              className={`!w-4 !h-4 transition-transform duration-200 ${isOpen ? "rotate-180" : "rotate-0"}`}
            />
          </>
        )}
      />
    </div>
  );
};

export default Nav;
