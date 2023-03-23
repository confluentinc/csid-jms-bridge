import { getCollection } from 'astro:content'

const posts = await getCollection('docs', p => {
  return !p.data.draft
})
const documents = posts.map(post => ({
  url: import.meta.env.BASE_URL + 'docs/' + post.slug,
  title: post.data.title,
  description: post.data.description,
  section: post.data.section,
  body: post.body,
}))

export async function get() {
  const body = JSON.stringify(documents)
  return {
    body,
  }
}
