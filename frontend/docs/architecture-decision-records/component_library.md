# Component Library

## Status

Approved

## Context

We would like to choose a component library to make frontend development easier and to allow our designer to design with the component library in mind. The components library should be: full-featured (have a lot of common/useful components), be customizable (especially related to style) and ideally look pretty good by default (if there is a default style).

We are considering a few options:

- React Spectrum
- React Aria
- Shadcn
- Radix UI

Both React Spectrum and Shadcn are meant to be pre-built, already-styled component libraries. React Spectrum has a lot of nice components, but unfortunately, from their docs:

> React Spectrum components are designed to be consistent across all Adobe applications. They include built-in styling that has been considered carefully, and extensively tested. In general, customizing Spectrum design is discouraged, but most components do offer control over layout and other aspects. In addition, you can use Spectrum defined variables to ensure your application conforms to design requirements, and is adaptive across platform scales and color schemes.

So that is probably not the best fit for us.

Shadcn on the other hand is designed to be customizable. It appears that they take Radix UI, add default styling to it and ship you the source code. That way you can modify the components/styling as you see fit. This seems like a great approach.

React Aria is very similar to Radix UI in that they are both headless and just ship component functionality and leave the styles up to you. React Aria does provide a Tailwind CSS starter kit which is very similar to what Shadcn provides: https://react-spectrum.adobe.com/react-aria-tailwind-starter/index.html

There are also other projects which add their flavor of starter styling to React Aria: https://github.com/zaichaopan/react-aria-components-tailwind-starter

In terms of React Aria vs Shadcn, React Aria has support for drag and drop (we are already using it). They both have a similar number of components.

Let's take an example, Accordion. React Spectrum has an accordion, but React Aria does not, so in order to use that, we'd have to copy the code from spectrum or build it ourselves. Shadcn has an Accordion component. They both have a Combobox component. Shadcn has a nice Sidebar component that is missing in React Aria. While React Aria has advanced primitives for building lists: https://react-spectrum.adobe.com/react-aria/GridList.html

Underneath Shadcn, Radix UI has a nice Data List component: https://www.radix-ui.com/themes/docs/components/data-list

Both libraries support styling by Tailwind CSS (which we are using).

## Decision

I don't want to invest the time to compare every single component, but Shadcn provides nice defaults (nicer looking than React Aria starter kit IMO) and is very popular. It also has some components that don't exist in React Aria (Accordion) while React Aria has some components that don't exist in Shadcn (Grid List). Radix UI also has some components not implemented in Shadcn, but we can drop down to Radix UI from Shadcn. For drag-and-drop behavior, we will likely continue using React Aria, but perhaps the dnd functionaltiy will not be as well integrated with the components as it is in e.g. React Aria's Grid List.

Someone mentioned this on Reddit regarding React Aria:

> I fundamentally have problems with their usePress event.
>
> They’ve reimplemented click events in JavaScript. I found this to be the source of numerous bugs and strange behaviours. Interacting with a site with react-aria usePress feels unnatural as pressing something behaves differently to every other website not using react-aria. So a strong no for future projects. I don’t need a library re-inventing something so core as clicking stuff.

- https://www.reddit.com/r/reactjs/comments/1css7vy/why_is_reactaria_not_talked_about_as_much_as/

Additionally, Karan has worked with Shadcn and enjoyed the experience.

So the winner for now goes to Shadcn:

- Nice looking defaults
- Good selection of components
- Developer familiarity
- Large community
- Not re-inventing the wheel regarding click events (NOTE: I haven't confirmed that React Aria does this)

## Consequences

We will see which components we wish we had and what problems we find once we get started. Then we can compare and see if React Aria provides solutions to those issues. Otherwise, we get some nice looking default components to get going with and they are easy to customize.
