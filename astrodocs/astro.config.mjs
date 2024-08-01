import {defineConfig} from 'astro/config'
import starlight from '@astrojs/starlight'
import starlightLinksValidatorPlugin from 'starlight-links-validator'
import starlightImageZoomPlugin from 'starlight-image-zoom'
import starlightVersions from 'starlight-versions'
import rehypeMermaid from 'rehype-mermaid'
import rehypeRaw from 'rehype-raw'
import robotsTxt from 'astro-robots-txt'

import tailwind from '@astrojs/tailwind'

// https://astro.build/config
export default defineConfig({
    markdown: {
        rehypePlugins: [rehypeRaw, rehypeMermaid]
    },
    build: {
        inlineStylesheets: 'always'
    },
    integrations: [starlight({
        plugins: [starlightLinksValidatorPlugin({errorOnRelativeLinks: false}), starlightImageZoomPlugin(), starlightVersions({
            versions: [{slug: "3.3.4"}],
        })],
        customCss: [
            './src/tailwind.css'
        ],
        lastUpdated: true,
        title: 'JMS Bridge Docs',
        description: 'JMS Bridge Documentation',
        favicon: '/src/assets/logo.svg',
        logo: {
            light: '/src/assets/logo.svg',
            dark: '/src/assets/logo.svg'
        },
        sidebar: [{
            label: 'Home',
            items: [{
                label: 'Introduction',
                link: '/'
            }]
        }, {
            label: 'Core Concepts',
            autogenerate: {
                directory: 'concepts'
            }
        }, {
            label: 'Guides',
            autogenerate: {
                directory: 'guides'
            }
        }, {
            label: 'Tutorials',
            autogenerate: {
                directory: 'tutorials'
            }
        }, {
            label: 'Reference',
            autogenerate: {
                directory: 'reference'
            }
        }]
    }), tailwind({
        // Disable the default base styles:
        applyBaseStyles: false
    }),
        robotsTxt({
            policy: [{
                userAgent: '*',
                disallow: ['/']
            }]
        }), tailwind()],
    site: 'https://congenial-dollop-j5lkkqp.pages.github.io/',
    base: '/'
})