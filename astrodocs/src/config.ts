import type { CollectionEntry } from 'astro:content'

export type Frontmatter = CollectionEntry<'docs'>['data']
export const SiteMetadata = {
  title: 'CSID JMS Bridge',
  description: 'Documentation for CSID JMS Bridge.',
  repository: 'https://github.com/confluentinc/csid-jms-bridge',
  social: [
    {
      name: 'Email',
      link: 'mailto:accelerators-encryption@confluent.io',
      icon: 'envelope',
    },
    {
      name: 'LinkedIn',
      link: 'https://www.linkedin.com/company/confluent',
      icon: 'linkedin',
    },
    {
      name: 'Twitter',
      link: 'https://twitter.com/confluentinc',
      icon: 'twitter',
    },
    {
      name: 'Github',
      link: 'https://github.com/confluentinc/csid-jms-bridge',
      icon: 'github',
    },
  ],
  buildTime: new Date(),
}

export const Logo = '../images/svg/confluentinc/logo.svg'

export const NavigationLinks = [
  { name: 'Docs', href: 'docs/get-started/introduction' },
]

export const PAGE_SIZE = 6

export const GITHUB_EDIT_URL = `https://github.com/confluentinc/csid-jms-bridge/astrodocs`

export type Sidebar = Record<string, { text: string; link: string }[]>
