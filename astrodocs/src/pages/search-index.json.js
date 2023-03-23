import { getCollection } from 'astro:content'
import lunr from 'lunr'

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
const idx = lunr(function () {
  this.ref('url')
  this.field('title')
  this.field('description')
  this.field('section')
  this.field('body')

  documents.forEach(function (doc) {
    this.add(doc)
  }, this)
})

export async function get() {
  const body = JSON.stringify(idx)
  return {
    body,
  }
}
