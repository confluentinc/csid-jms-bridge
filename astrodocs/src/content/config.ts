import { defineCollection } from 'astro:content';
import { docsSchema, i18nSchema } from '@astrojs/starlight/schema';
import { docsVersionsSchema } from 'starlight-versions/schema'

export const collections = {
	docs: defineCollection({ schema: docsSchema() }),
	i18n: defineCollection({ type: 'data', schema: i18nSchema() }),
	versions: defineCollection({ type: 'data', schema: docsVersionsSchema() })
};