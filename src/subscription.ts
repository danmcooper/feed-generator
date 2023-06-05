import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import { AtpAgent } from '@atproto/api'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(
    evt: RepoEvent,
    agent: AtpAgent | undefined,
    MAX_THRESHOLD: number,
    MIN_THRESHOLD: number,
    MIN_AGE_OF_POST_IN_MS: number,
    MAX_AGE_OF_POST_IN_MS: number,
  ) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    // console.log(`MAX_THRESHOLD: ${MAX_THRESHOLD}`)
    // console.log(`MIN_THRESHOLD: ${MIN_THRESHOLD}`)

    const { postsToCreate, postsToDelete } = process(
      ops,
      agent,
      MAX_THRESHOLD,
      MIN_THRESHOLD,
      MIN_AGE_OF_POST_IN_MS,
      MAX_AGE_OF_POST_IN_MS,
    )

    if (postsToDelete.length > 0) {
      console.log(`DELETE: ${JSON.stringify(postsToDelete, null, 2)}`)
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()

      showCount(this.db)
    }
    if (postsToCreate.length > 0) {
      console.log(`CREATE: ${JSON.stringify(postsToCreate, null, 2)}`)
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()

      showCount(this.db)
    }
  }
}

const showCount = async (db) => {
  const { count } = db.fn
  const posts = await db.selectFrom('post').select(count('cid')).execute()
  console.log(`post count: ${posts[0]['count("cid")']}`)
}

const postsByUri = {}
const postsInDBByHour = {}
let currentHourIndex = 0

const process = (
  ops,
  agent,
  MAX_THRESHOLD,
  MIN_THRESHOLD,
  MIN_AGE_OF_POST_IN_MS,
  MAX_AGE_OF_POST_IN_MS,
) => {
  let postsToDelete = ops.posts.deletes
    .map((del) => {
      if (postsByUri[del.uri]) {
        if (postsByUri[del.uri].added) {
          delete postsByUri[del.uri]
          return del.uri
        } else {
          delete postsByUri[del.uri]
        }
      }
      return null
    })
    .filter((uri) => uri !== null)
  let postsToCreate: any = []

  const currentHour = new Date().getHours()

  if (currentHourIndex !== currentHour) {
    postsToDelete = [...postsToDelete, ...(postsInDBByHour[currentHour] ?? [])]
    console.log(
      `deleting posts from hour ${currentHour}: ${JSON.stringify(
        postsInDBByHour[currentHour],
        null,
        2,
      )}`,
    )
    currentHourIndex = currentHour
    postsInDBByHour[currentHourIndex] = []
    cleanupOlderThan23Hours(postsByUri)
  }

  for (const post of ops.posts.creates) {
    // ignore if reply, hey, my feed my rules
    if (post.record.reply) continue

    // add to map
    postsByUri[post.uri] = {
      post,
      likes: 0,
      added: false,
      time: new Date().getTime(),
    }
  }

  for (const like of ops.likes.creates) {
    if (postsByUri[like.record.subject.uri]) {
      // console.log('jjj')
      // console.log(postsByUri[like.record.subject.uri].time / 1000)
      // console.log((new Date().getTime() - MIN_AGE_OF_POST_IN_MS) / 1000)

      postsByUri[like.record.subject.uri].likes++
      if (
        exceedsThreshold(postsByUri[like.record.subject.uri], MAX_THRESHOLD)
      ) {
        postsToDelete = [...postsToDelete, like.record.subject.uri]
        delete postsByUri[like.record.subject.uri]
      } else if (
        entersThreshold(
          postsByUri[like.record.subject.uri],
          MIN_THRESHOLD,
          MIN_AGE_OF_POST_IN_MS,
          MAX_AGE_OF_POST_IN_MS,
        )
      ) {
        // add in
        postsToCreate = [
          ...postsToCreate,
          postsByUri[like.record.subject.uri].post,
        ]
        postsByUri[like.record.subject.uri].added = true
        postsInDBByHour[currentHourIndex] = [
          ...postsInDBByHour[currentHourIndex],
          like.record.subject.uri,
        ]
      }
    }
  }

  postsToCreate = postsToCreate.map((create) => {
    return {
      uri: create.uri,
      cid: create.cid,
      replyParent: create.record?.reply?.parent.uri ?? null,
      replyRoot: create.record?.reply?.root.uri ?? null,
      indexedAt: new Date().toISOString(),
    }
  })

  return { postsToDelete, postsToCreate }
}

const exceedsThreshold = (post, maxThreshold) => {
  return post.likes > maxThreshold && post.added
}

const entersThreshold = (post, minThreshold, minAge, maxAge) => {
  const now = new Date().getTime()
  return (
    post.likes > minThreshold &&
    !post.added &&
    post.time < now - minAge &&
    post.time > now - maxAge
  )
}

const cleanupOlderThan23Hours = (postsByUri) => {
  Object.keys(postsByUri).forEach((uri) => {
    if (postsByUri[uri].time < new Date().getTime() - 23 * 60 * 60 * 1000) {
      console.log(`removing post ${uri} from memory`)
      delete postsByUri[uri]
    }
  })
}
