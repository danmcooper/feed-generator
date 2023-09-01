import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import {
  FirehoseSubscriptionBase,
  getOpsByType,
  rejectedLanguages,
} from './util/subscription'
import { AtpAgent } from '@atproto/api'

const rejectList = {}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(
    evt: RepoEvent,
    agent: AtpAgent | undefined,
    MAX_THRESHOLD: number,
    MIN_THRESHOLD: number,
    MIN_AGE_OF_POST_IN_MS: number,
    MAX_AGE_OF_POST_IN_MS: number,
    maxFollowersAllowed: number,
  ) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    const { postsToCreate, postsToDelete } = await processIt(
      ops,
      agent,
      MAX_THRESHOLD,
      MIN_THRESHOLD,
      MIN_AGE_OF_POST_IN_MS,
      MAX_AGE_OF_POST_IN_MS,
      maxFollowersAllowed,
      this.db,
    )

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
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

const processIt = async (
  ops,
  agent,
  MAX_THRESHOLD,
  MIN_THRESHOLD,
  MIN_AGE_OF_POST_IN_MS,
  MAX_AGE_OF_POST_IN_MS,
  maxFollowersAllowed,
  db,
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
    await showCount(db)
  }

  for (const post of ops.posts.creates) {
    if (rejectList[post.author]) {
      continue
    }

    const author = await agent?.api.app.bsky.actor.getProfile({
      actor: post.author,
    })

    // console.log(`author: ${JSON.stringify(author, null, 2)}`)

    if (rejectPost(post, author, maxFollowersAllowed)) continue

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
          MAX_THRESHOLD,
          MIN_AGE_OF_POST_IN_MS,
          MAX_AGE_OF_POST_IN_MS,
        )
      ) {
        const fullPost = await agent?.api.app.bsky.feed.getPosts({
          uris: [like.record.subject.uri],
        })

        if (passesCheck(fullPost)) {
          postsToCreate = [
            ...postsToCreate,
            postsByUri[like.record.subject.uri].post,
          ]
          postsByUri[like.record.subject.uri].added = true
          postsInDBByHour[currentHourIndex] = [
            ...postsInDBByHour[currentHourIndex],
            like.record.subject.uri,
          ]
        } else {
          rejectList[fullPost.data.posts[0].author.did] = true
          delete postsByUri[fullPost.data.posts[0].uri]
        }
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

const entersThreshold = (post, minThreshold, maxThreshold, minAge, maxAge) => {
  const now = new Date().getTime()
  return (
    post.likes > minThreshold &&
    post.likes <= maxThreshold &&
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

function profileContainsTerms(text) {
  const textLower = text?.toLowerCase() ?? ''
  const forbiddenTerms =
    /(nsfw|🔞|🦊|fursuit|ffxiv|boobs|onlyfans|pervert|himbo|dni|transformation|paws|lewd|18\+|\+18|18\↑|shirtless|Onlyfans|only\s*fans|of\s*model|thirst|fur|daddy|nudist|sub|subby|dom|domme|masochist|horny|furry|fursuit|anthro|porn|penis|cock|tits|nude|swer|suggestive|no\s*minors)/i
  return forbiddenTerms.test(textLower)
}

// const hasImage = (post) => {
//   return post.record?.embed?.images?.length > 0
// }

const LanguageDetect = require('languagedetect')
const lngDetector = new LanguageDetect()

const languageReject = (text) => {
  const languages = lngDetector.detect(text)
  if (languages.length > 0) {
    if (rejectedLanguages.includes(languages[0][0])) {
      return true
    }
  }
  return false
}

const rejectPost = (post, author, maxFollowersAllowed) => {
  const textLower = post.record.text.toLowerCase()
  if (author.data.followersCount > maxFollowersAllowed) {
    rejectList[post.author] = true
    return true
  }
  if (author.data.viewer.muted || author.data.viewer.blockedBy) {
    rejectList[post.author] = true
    return true
  }
  if (profileContainsTerms(author.data.description)) {
    // if (hasImage(post) && profileContainsTerms(author.data.description)) {
    console.log(`rejecting ${author.data.description}`)
    rejectList[post.author] = true
    return true
  }
  if (languageReject(textLower)) {
    rejectList[post.author] = true
    return true
  }

  const regex = /#.*fur.*?/gi
  if (
    author.data.postsCount < 4 ||
    post.record.reply ||
    textLower.includes('hello world') ||
    textLower.includes('hello, world') ||
    textLower.match(regex)?.length > 0 ||
    containsTheseHashtags(textLower)
  ) {
    return true
  }
  return false
}

function containsTheseHashtags(text) {
  const hashtags = /#bondage|#bdsm|#nsfw|#gay|#yiff|#dirtypaws|#anthro|#porn/i
  return hashtags.test(text)
}
const passesCheck = (fullPost) => {
  if (fullPost.data.posts[0]?.labels[0]?.val?.length > 0) {
    return false
  }
  return true
}
