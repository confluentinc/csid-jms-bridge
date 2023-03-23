// 1. Import your utilities and schemas
import { z, defineCollection } from 'astro:content'

// 2. Define your collections
const docCollection = defineCollection({
  schema: z.object({
    draft: z.boolean().optional(),
    section: z.string(),
    weight: z.number().default(0),
    title: z.string(),
    description: z.string(),
    images: z.array(z.string()).optional(),
    extra: z.array(z.enum(['markmap', 'mermaid'])).optional(),
  }),
})

// 3. Export multiple collections to register them
export const collections = {
  docs: docCollection,
}
