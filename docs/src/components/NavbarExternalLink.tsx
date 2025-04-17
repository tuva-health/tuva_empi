import React from "react";
import Link from "@docusaurus/Link";
import { ExternalLink } from "lucide-react";

export interface NavbarExternalLinkProps {
  href: string;
  label: string;
}

const NavbarExternalLink: React.FC<NavbarExternalLinkProps> = ({
  href,
  label,
}) => {
  return (
    <Link
      href={href}
      className="navbar__item navbar__link"
      target="_blank"
      rel="noopener noreferrer"
      style={{
        display: "flex",
        alignItems: "center",
        gap: "0.4rem",
      }}
    >
      {label}
      <ExternalLink size={16} strokeWidth={1.8} />
    </Link>
  );
};

export default NavbarExternalLink;
