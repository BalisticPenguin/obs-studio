#include <obs-module.h>
#include <util/threading.h>
#include <util/platform.h>
#include <util/darray.h>
#include <util/dstr.h>

#define do_log(level, format, ...)                \
	blog(level, "[slideshow2: '%s'] " format, \
	     obs_source_get_name(ss->source), ##__VA_ARGS__)

#define debug(format, ...) do_log(LOG_DEBUG, format, ##__VA_ARGS__)
#define warn(format, ...) do_log(LOG_WARNING, format, ##__VA_ARGS__)

/* clang-format off */

#define S_TR_SPEED                     "transition_speed"
#define S_CUSTOM_SIZE                  "use_custom_size"
#define S_SLIDE_TIME                   "slide_time"
#define S_TRANSITION                   "transition"
#define S_RANDOMIZE                    "randomize"
#define S_LOOP                         "loop"
#define S_HIDE                         "hide"
#define S_FILES                        "files"
#define S_BEHAVIOR                     "playback_behavior"
#define S_BEHAVIOR_STOP_RESTART        "stop_restart"
#define S_BEHAVIOR_PAUSE_UNPAUSE       "pause_unpause"
#define S_BEHAVIOR_ALWAYS_PLAY         "always_play"
#define S_SLIDEMODE                    "slide_mode"
#define S_SLIDEMODE_AUTO               "mode_auto"
#define S_SLIDEMODE_MANUAL             "mode_manual"
#define S_LOADMODE		       "load_mode"
#define S_LOADMODE_PRELOAD             "preload"
#define S_LOADMODE_ONDEMAND            "on_demand"

#define TR_CUT                         "cut"
#define TR_FADE                        "fade"
#define TR_SWIPE                       "swipe"
#define TR_SLIDE                       "slide"

/* Recycle translations from SlideShow 1 for now. */
#define T_(text) obs_module_text("SlideShow." text)
#define T_TR_SPEED                     T_("TransitionSpeed")
#define T_CUSTOM_SIZE                  T_("CustomSize")
#define T_SLIDE_TIME                   T_("SlideTime")
#define T_TRANSITION                   T_("Transition")
#define T_RANDOMIZE                    T_("Randomize")
#define T_LOOP                         T_("Loop")
#define T_HIDE                         T_("HideWhenDone")
#define T_FILES                        T_("Files")
#define T_BEHAVIOR                     T_("PlaybackBehavior")
#define T_BEHAVIOR_STOP_RESTART        T_("PlaybackBehavior.StopRestart")
#define T_BEHAVIOR_PAUSE_UNPAUSE       T_("PlaybackBehavior.PauseUnpause")
#define T_BEHAVIOR_ALWAYS_PLAY         T_("PlaybackBehavior.AlwaysPlay")
#define T_MODE                         T_("SlideMode")
#define T_MODE_AUTO                    T_("SlideMode.Auto")
#define T_MODE_MANUAL                  T_("SlideMode.Manual")
#define T_LOADMODE                     T_("LoadMode")
#define T_LOADMODE_PRELOAD             T_("LoadMode.Preload")
#define T_LOADMODE_ONDEMAND            T_("LoadMode.OnDemand")

#define T_TR_(text) obs_module_text("SlideShow.Transition." text)
#define T_TR_CUT                       T_TR_("Cut")
#define T_TR_FADE                      T_TR_("Fade")
#define T_TR_SWIPE                     T_TR_("Swipe")
#define T_TR_SLIDE                     T_TR_("Slide")

/* clang-format on */

/* ------------------------------------------------------------------------- */

extern uint64_t image_source_get_memory_usage(void *data);

#define BYTES_TO_MBYTES (1024 * 1024)
#define MAX_MEM_USAGE (400 * BYTES_TO_MBYTES)

/* How many past items to cache before the current item. */
#define CACHE_BEFORE 2
/* How many items to cache in front of the current item. */
#define CACHE_AFTER 2

/* How to handle image loading. */
enum slideshow_mode {
	/* All images are loaded immediately up until a memory limit is hit. */
	SLIDESHOW_MODE_PRELOAD,
	/* Images are loaded on-demand in a worker thread. */
	SLIDESHOW_MODE_ON_DEMAND,
};

enum behavior {
	BEHAVIOR_STOP_RESTART,
	BEHAVIOR_PAUSE_UNPAUSE,
	BEHAVIOR_ALWAYS_PLAY,
};

struct entry {
	/* Path of the loaded source to double-check cache consistency. */
	char *path;
	/* The loaded image source. */
	obs_source_t *source;
};

/* Name a few dynamic array types for typesafe handling. */
typedef DARRAY(char *) DARRAY_char_p;
typedef DARRAY(size_t) DARRAY_size_t;
typedef DARRAY(struct entry) DARRAY_entry;
typedef DARRAY(obs_source_t *) DARRAY_obs_source_t_p;

/* Configuration derived from the user's settings. */
struct slideshow2_config {
	enum slideshow_mode mode;
	enum behavior behavior;
	bool randomize;
	bool loop;
	bool manual;
	bool hide;
	float slide_time;
	uint32_t tr_speed;
	char *tr_name;

	uint32_t cx;
	uint32_t cy;

	DARRAY_char_p file_paths;
};

/* Main struct. */
struct slideshow2 {
	obs_source_t *source;

	pthread_mutex_t mutex;
	/* Used for debugging. May be used with assertions to give a hint that
	 * a function may only be called with the mutex held. You can somewhat
	 * reliably `assert(ss->have_mutex)`, but testing for `!ss->have_mutex`
	 * may yield false-negatives!
	 */
	volatile bool have_mutex;

	/* Variables that need mutex lock */

	struct slideshow2_config config;
	uint64_t mem_usage;
	bool restart_on_activate;
	bool pause_on_deactivate;
	bool use_cut;
	bool paused;
	bool stop;
	obs_source_t *transition;

	float elapsed;

	/* File paths and their image sources. */
	DARRAY_entry entries;
	/* Index marking the item that should be displayed. */
	size_t cur_item;
	/* Indices of entries that should be cached. */
	DARRAY_size_t cache_queue;
	/* Sorted, deduplicated list of indices to cache. */
	DARRAY_size_t cached_entries_set;

	bool retry_transition;

	/* Variables that can do without mutex lock */

	os_event_t *cache_event;
	pthread_t cache_thread;
	bool cache_thread_created;
	volatile bool shutdown;

	obs_hotkey_id play_pause_hotkey;
	obs_hotkey_id restart_hotkey;
	obs_hotkey_id stop_hotkey;
	obs_hotkey_id next_hotkey;
	obs_hotkey_id prev_hotkey;
};

/* ------------------------------------------------------------------------- */
/* Utilities */

static void lock_mutex(struct slideshow2 *ss)
{
	pthread_mutex_lock(&ss->mutex);
	assert(!ss->have_mutex);
	ss->have_mutex = true;
}

static void unlock_mutex(struct slideshow2 *ss)
{
	assert(ss->have_mutex);
	ss->have_mutex = false;
	pthread_mutex_unlock(&ss->mutex);
}

static int sort_size_t(const void *p1, const void *p2)
{
	return *((const size_t *)p1) - *((const size_t *)p2);
}

/* ------------------------------------------------------------------------- */
/* Obtain sources */

static obs_source_t *obtain_transition(struct slideshow2 *ss)
{
	obs_source_t *tr;

	lock_mutex(ss);
	tr = ss->transition;
	obs_source_addref(tr);
	unlock_mutex(ss);

	return tr;
}

static obs_source_t *obtain_cached_source(struct slideshow2 *ss, size_t index)
{
	assert(ss->have_mutex);

	if (index >= ss->entries.num)
		return NULL;

	struct entry *entry = &ss->entries.array[index];

	obs_source_addref(entry->source);
	return entry->source;
}

/* ------------------------------------------------------------------------- */
/* Manage files */

static obs_source_t *create_source_from_file(const char *file)
{
	obs_data_t *settings = obs_data_create();
	obs_source_t *source;

	obs_data_set_string(settings, "file", file);
	obs_data_set_bool(settings, "unload", false);
	source = obs_source_create_private("image_source", NULL, settings);

	obs_data_release(settings);
	return source;
}

static void free_files(DARRAY_char_p *file_paths)
{
	for (size_t i = 0; i < file_paths->num; i++) {
		bfree(file_paths->array[i]);
	}

	da_free((*file_paths));
}

/* ------------------------------------------------------------------------- */
/* Manage current index */

static size_t random_index(struct slideshow2 *ss, size_t avoid)
{
	size_t result;
	size_t num_entries = ss->entries.num;
	if (num_entries <= 1)
		return 0;

	// Prevent repeating the same image.
	do {
		// Avoid modulo bias by retrying if the result is outside
		// the range divisible by the upper bound.
		do {
			result = rand();
		} while (result >= RAND_MAX - (RAND_MAX % num_entries));

		result %= num_entries;
	} while (result == avoid);

	return result;
}

static size_t index_after(struct slideshow2 *ss, size_t cursor)
{
	size_t result = cursor + 1;
	return result % ss->entries.num;
}

static size_t index_before(struct slideshow2 *ss, size_t cursor)
{
	size_t num_entries = ss->entries.num;
	if (num_entries == 0)
		return 0;

	if (cursor == 0) {
		return num_entries - 1;
	} else {
		return cursor - 1;
	}
}

static void fill_empty_cache(struct slideshow2 *ss)
{
	size_t cursor = ss->cur_item;
	bool randomize = ss->config.randomize;

	for (size_t i = 0; i < CACHE_BEFORE; ++i) {
		if (randomize) {
			cursor = random_index(ss, cursor);
		} else {
			cursor = index_before(ss, cursor);
		}
		da_insert(ss->cache_queue, 0, &cursor);
	}

	da_push_back(ss->cache_queue, &ss->cur_item);

	cursor = ss->cur_item;
	for (size_t i = 0; i < CACHE_AFTER; ++i) {
		if (randomize) {
			cursor = random_index(ss, cursor);
		} else {
			cursor = index_after(ss, cursor);
		}
		da_push_back(ss->cache_queue, &cursor);
	}
}

static void set_cur_item(struct slideshow2 *ss, size_t index)
{
	assert(ss->have_mutex);

	if (ss->cur_item == index)
		return;

	ss->cur_item = index;

	if (ss->config.mode == SLIDESHOW_MODE_ON_DEMAND) {
		da_resize(ss->cache_queue, 0);
		fill_empty_cache(ss);
		os_event_signal(ss->cache_event);
	}
}

static void set_next_item(struct slideshow2 *ss)
{
	assert(ss->have_mutex);

	if (ss->entries.num == 0)
		return;

	if (ss->config.mode == SLIDESHOW_MODE_PRELOAD) {
		/* For preload mode, it's very simple. */
		if (ss->config.randomize) {
			ss->cur_item = random_index(ss, ss->cur_item);
		} else {
			ss->cur_item = index_after(ss, ss->cur_item);
		}
		return;
	}

	/* On-demand mode requires more sophisticated handling. The cache is
	 * always holding `CACHE_BEFORE + 1 + CACHE_AFTER` number of
	 * indices. Future indices are pushed to this list and old entries
	 * discarded. 
	 */

	if (ss->cache_queue.num == 0) {
		/* Cache is empty, fill it for the first time. */
		fill_empty_cache(ss);
	}

	assert(ss->cache_queue.num = CACHE_BEFORE + 1 + CACHE_AFTER);
	size_t last_index = ss->cache_queue.array[ss->cache_queue.num - 1];
	size_t next_index;
	if (ss->config.randomize) {
		next_index = random_index(ss, last_index);
	} else {
		next_index = index_after(ss, last_index);
	}
	da_push_back(ss->cache_queue, &next_index);
	da_erase(ss->cache_queue, 0);

	ss->cur_item = ss->cache_queue.array[CACHE_BEFORE];
	os_event_signal(ss->cache_event);
}

static void set_previous_item(struct slideshow2 *ss)
{
	assert(ss->have_mutex);

	if (ss->entries.num == 0)
		return;

	if (ss->config.mode == SLIDESHOW_MODE_PRELOAD) {
		/* For preload mode, it's very simple. */
		if (ss->config.randomize) {
			ss->cur_item = random_index(ss, ss->cur_item);
		} else {
			ss->cur_item = index_before(ss, ss->cur_item);
		}
		return;
	}

	/* On-demand mode requires more sophisticated handling. The cache is
	 * always holding `CACHE_BEFORE + 1 + CACHE_AFTER` number of
	 * indices. Future indices are pushed to this list and old entries
	 * discarded. 
	 */

	if (ss->cache_queue.num == 0) {
		/* Cache is empty, fill it for the first time. */
		fill_empty_cache(ss);
	}

	assert(ss->cache_queue.num = CACHE_BEFORE + 1 + CACHE_AFTER);
	size_t first_index = ss->cache_queue.array[0];
	size_t previous_index;
	if (ss->config.randomize) {
		previous_index = random_index(ss, first_index);
	} else {
		previous_index = index_before(ss, first_index);
	}
	da_insert(ss->cache_queue, 0, &previous_index);
	da_pop_back(ss->cache_queue);

	ss->cur_item = ss->cache_queue.array[CACHE_BEFORE];
	os_event_signal(ss->cache_event);
}

/* ------------------------------------------------------------------------- */
/* Cache worker thread */

static void update_cached_entries_set(struct slideshow2 *ss, DARRAY_size_t *set)
{
	assert(ss->have_mutex);

	da_copy((*set), ss->cache_queue);

	if (set->num <= 1)
		return;

	qsort(set->array, set->num, sizeof(size_t), sort_size_t);

	/* Deduplicate. */
	for (size_t index = 0; index < set->num - 1;) {
		size_t element1 = set->array[index];
		size_t element2 = set->array[index + 1];
		if (element1 == element2) {
			da_erase((*set), index);
		} else {
			index++;
		}
	}
}

static void calculate_cache_actions(DARRAY_size_t const *old_set,
				    DARRAY_size_t const *new_set,
				    DARRAY_size_t *evict, DARRAY_size_t *add)
{
	for (size_t o_index = 0; o_index < old_set->num; ++o_index) {
		size_t candidate = old_set->array[o_index];
		size_t n_index = da_find((*new_set), &candidate, 0);

		if (n_index == DARRAY_INVALID) {
			da_push_back((*evict), &candidate);
		}
	}

	for (size_t n_index = 0; n_index < new_set->num; ++n_index) {
		size_t candidate = new_set->array[n_index];
		size_t o_index = da_find((*old_set), &candidate, 0);

		if (o_index == DARRAY_INVALID) {
			da_push_back((*add), &candidate);
		}
	}
}

static bool next_index_to_cache(struct slideshow2 *ss, DARRAY_size_t *indices,
				size_t *out_index)
{
	assert(ss->have_mutex);

	if (indices->num == 0)
		return false;

	/* Always prefer the current item, if it's in the list of indices. */
	if (da_find((*indices), &ss->cur_item, 0) != DARRAY_INVALID) {
		*out_index = ss->cur_item;
		return true;
	}

	// TODO: We should be more intelligent here and look at ss->cache_queue:
	// * First, try to cache the current item (already done above).
	// * Then, try to cache the upcoming items.
	// * Only then try to cache past items.

	*out_index = indices->array[0];
	return true;
}

static void evict_sources(struct slideshow2 *ss, DARRAY_size_t *evict,
			  DARRAY_obs_source_t_p *cleanup)
{
	assert(ss->have_mutex);
	struct entry *entries = ss->entries.array;

	for (size_t i = 0; i < evict->num; ++i) {
		size_t entry_index = evict->array[i];
		/* If the source is NULL we have a logic error somewhere. */
		assert(entries[entry_index].source != NULL);

		da_push_back((*cleanup), &entries[entry_index].source);
		entries[entry_index].source = NULL;

		size_t index = da_find(ss->cached_entries_set, &entry_index, 0);
		assert(index != DARRAY_INVALID);
		if (index != DARRAY_INVALID) {
			da_erase(ss->cached_entries_set, index);
		}
	}
}

static void load_and_cache(struct slideshow2 *ss, size_t entry_index,
			   const char *path)
{
	obs_source_t *source = create_source_from_file(path);
	if (source == NULL) {
		warn("Failed to load %s", path);
		/* How to procede? We're going to be stuck until cur_item
		 * changes as in the next cache_thread iteration we're going
		 * to try to load the same entry again. Need some way to signal
		 * this error condition so that at least the succeeding entries
		 * are going to get cached. Or we could remove the whole entry
		 * for this file.
		 */
		return;
	}

	bool need_to_release = false;

	lock_mutex(ss);
	DARRAY_entry *entries = &ss->entries;
	DARRAY_size_t *cached_entries_set = &ss->cached_entries_set;

	if (entry_index < entries->num &&
	    strcmp(entries->array[entry_index].path, path) == 0) {
		entries->array[entry_index].source = source;

		/* Entry may not be in the set yet, otherwise we have a logic error. */
		assert(da_find((*cached_entries_set), &entry_index, 0) ==
		       DARRAY_INVALID);
		da_push_back((*cached_entries_set), &entry_index);
		qsort(cached_entries_set->array, cached_entries_set->num,
		      sizeof(size_t), sort_size_t);

	} else {
		/* The entries have updated and the index we've loaded is no longer valid.
		 * We could try to find the path in the new entries so the work wasn't in
		 * vain but it might not be what's wanted and the source might get evicted
		 * in the next iteration anyway. So discard it.
		 */
		need_to_release = true;
	}
	unlock_mutex(ss);

	if (need_to_release)
		obs_source_release(source);
}

static void *cache_thread(void *opaque)
{
	struct slideshow2 *ss = opaque;

	os_set_thread_name("slideshow: cache worker thread");

	while (!ss->shutdown) {
		os_event_wait(ss->cache_event);

		DARRAY_size_t old_set;
		DARRAY_size_t new_set;
		DARRAY_size_t evict;
		DARRAY_size_t add;
		DARRAY_obs_source_t_p cleanup;
		da_init(old_set);
		da_init(new_set);
		da_init(evict);
		da_init(add);
		da_init(cleanup);

		size_t entry_index = 0;
		char *path = NULL;

		lock_mutex(ss);
		{
			da_copy(old_set, ss->cached_entries_set);
			update_cached_entries_set(ss, &new_set);

			// TODO: It would be better not to do any expensive work
			// here. Just copy all the data needed and do the rest
			// outside the mutex.

			calculate_cache_actions(&old_set, &new_set, &evict,
						&add);

			if (next_index_to_cache(ss, &add, &entry_index)) {
				path = bstrdup(
					ss->entries.array[entry_index].path);
			}

			evict_sources(ss, &evict, &cleanup);
		}
		unlock_mutex(ss);

		/* Release the sources outside the lock to avoid a deadlock with
		 * the graphics thread.
		 */
		for (size_t i = 0; i < cleanup.num; i++) {
			obs_source_release(cleanup.array[i]);
		}
		da_free(cleanup);
		da_free(evict);
		da_free(add);
		da_free(new_set);
		da_free(old_set);

		if (path == NULL) {
			os_event_reset(ss->cache_event);
			continue;
		}

		load_and_cache(ss, entry_index, path);
		bfree(path);
	}

	return NULL;
}

/* ------------------------------------------------------------------------- */
/* Entries management */

static void clear_all_entries(struct slideshow2 *ss, DARRAY_entry *cleanup)
{
	/* Usually, we do have the mutex here and want to move stuff into a
	 * cleanup array to do potentially expensive work outside the mutex.
	 * But on dealloc, we don't have the mutex.
	 */

	da_move((*cleanup), ss->entries);
	da_resize(ss->cache_queue, 0);
	da_resize(ss->cached_entries_set, 0);
	ss->mem_usage = 0;
}

static void free_entries(DARRAY_entry *cleanup)
{
	size_t num = cleanup->num;
	struct entry *entries = cleanup->array;
	for (size_t i = 0; i < num; ++i) {
		obs_source_release(entries[i].source);
		bfree(entries[i].path);
	}
	da_resize((*cleanup), 0);
}

static void update_entries(struct slideshow2 *ss, bool preload,
			   DARRAY_char_p *file_paths, DARRAY_entry *new_entries,
			   uint64_t *mem_usage)
{
	size_t num_files = file_paths->num;
	for (size_t i = 0; i < num_files; ++i) {
		struct entry entry = {0};
		const char *path = file_paths->array[i];

		if (preload) {
			debug("Preloading %s", path);
			obs_source_t *source = create_source_from_file(path);
			if (source == NULL) {
				warn("Failed to load %s", path);
				continue;
			}
			entry.source = source;

			void *source_data = obs_obj_get_data(source);
			*mem_usage +=
				image_source_get_memory_usage(source_data);
		}
		entry.path = bstrdup(path);

		da_push_back((*new_entries), &entry);

		if (*mem_usage >= MAX_MEM_USAGE) {
			warn("Reached memory limit, loaded %zu of %zu images.",
			     new_entries->num, file_paths->num);
			break;
		}
	}
}

/* ------------------------------------------------------------------------- */
/* Transition management */

static inline bool item_valid(struct slideshow2 *ss)
{
	assert(ss->have_mutex);
	return ss->entries.num > 0 && ss->cur_item < ss->entries.num;
}

static bool do_transition(struct slideshow2 *ss, bool to_null)
{
	assert(ss->have_mutex);
	bool valid = item_valid(ss);

	if (valid && ss->use_cut) {
		obs_source_t *source = obtain_cached_source(ss, ss->cur_item);
		if (!source) {
			debug("No cached source, need to retry");
			ss->retry_transition = true;
			return false;
		}
		obs_transition_set(ss->transition, source);
		obs_source_release(source);

	} else if (valid && !to_null) {
		obs_source_t *source = obtain_cached_source(ss, ss->cur_item);
		if (!source) {
			debug("No cached source, need to retry");
			ss->retry_transition = true;
			return false;
		}
		obs_transition_start(ss->transition, OBS_TRANSITION_MODE_AUTO,
				     ss->config.tr_speed, source);
		obs_source_release(source);

	} else {
		obs_transition_start(ss->transition, OBS_TRANSITION_MODE_AUTO,
				     ss->config.tr_speed, NULL);
	}

	ss->retry_transition = false;
	return true;
}

/* ------------------------------------------------------------------------- */
/* Config update */

static bool valid_extension(const char *ext)
{
	if (!ext)
		return false;
	return astrcmpi(ext, ".bmp") == 0 || astrcmpi(ext, ".tga") == 0 ||
	       astrcmpi(ext, ".png") == 0 || astrcmpi(ext, ".jpeg") == 0 ||
	       astrcmpi(ext, ".jpg") == 0 || astrcmpi(ext, ".gif") == 0;
}

static void add_file(DARRAY_char_p *files_list, const char *path)
{
	char *copied_path = bstrdup(path);
	da_push_back((*files_list), &copied_path);
}

static void read_file_list(obs_data_array_t *files_from_settings,
			   DARRAY_char_p *files_list)
{
	size_t count = obs_data_array_count(files_from_settings);

	for (size_t i = 0; i < count; i++) {
		obs_data_t *item = obs_data_array_item(files_from_settings, i);
		const char *path = obs_data_get_string(item, "value");
		os_dir_t *dir = os_opendir(path);

		if (dir) {
			struct dstr dir_path = {0};
			struct os_dirent *ent;

			for (;;) {
				const char *ext;

				ent = os_readdir(dir);
				if (!ent)
					break;
				if (ent->directory)
					continue;

				ext = os_get_path_extension(ent->d_name);
				if (!valid_extension(ext))
					continue;

				dstr_copy(&dir_path, path);
				dstr_cat_ch(&dir_path, '/');
				dstr_cat(&dir_path, ent->d_name);
				add_file(files_list, dir_path.array);
			}

			dstr_free(&dir_path);
			os_closedir(dir);
		} else {
			add_file(files_list, path);
		}

		obs_data_release(item);
	}
}

static void parse_resolution(const char *res_str, uint32_t *out_cx,
			     uint32_t *out_cy)
{
	bool aspect_only = false;
	int cx_in = 0, cy_in = 0;

	int ret = sscanf(res_str, "%dx%d", &cx_in, &cy_in);
	if (ret == 2) {
		aspect_only = false;
	} else {
		ret = sscanf(res_str, "%d:%d", &cx_in, &cy_in);
		if (ret == 2) {
			aspect_only = true;
		}
	}

	double cx_f = 0;
	double cy_f = 0;

	double old_aspect = cx_f / cy_f;
	double new_aspect = (double)cx_in / (double)cy_in;

	if (aspect_only) {
		if (fabs(old_aspect - new_aspect) > EPSILON) {
			if (new_aspect > old_aspect)
				*out_cx = (uint32_t)(cy_f * new_aspect);
			else
				*out_cy = (uint32_t)(cx_f / new_aspect);
		}
	} else {
		*out_cx = (uint32_t)cx_in;
		*out_cy = (uint32_t)cy_in;
	}
}

static void read_config(obs_data_t *settings, struct slideshow2_config *config)
{
	obs_data_array_t *files_array;
	const char *tr_name;
	uint32_t new_duration;
	uint32_t new_speed;
	const char *behavior;
	const char *slide_mode;
	const char *load_mode;

	/* ------------------------------------- */
	/* get settings data */

	behavior = obs_data_get_string(settings, S_BEHAVIOR);

	if (astrcmpi(behavior, S_BEHAVIOR_PAUSE_UNPAUSE) == 0)
		config->behavior = BEHAVIOR_PAUSE_UNPAUSE;
	else if (astrcmpi(behavior, S_BEHAVIOR_ALWAYS_PLAY) == 0)
		config->behavior = BEHAVIOR_ALWAYS_PLAY;
	else /* S_BEHAVIOR_STOP_RESTART */
		config->behavior = BEHAVIOR_STOP_RESTART;

	load_mode = obs_data_get_string(settings, S_LOADMODE);
	slide_mode = obs_data_get_string(settings, S_SLIDEMODE);

	config->manual = (astrcmpi(slide_mode, S_SLIDEMODE_MANUAL) == 0);

	tr_name = obs_data_get_string(settings, S_TRANSITION);
	if (astrcmpi(tr_name, TR_CUT) == 0)
		tr_name = "cut_transition";
	else if (astrcmpi(tr_name, TR_SWIPE) == 0)
		tr_name = "swipe_transition";
	else if (astrcmpi(tr_name, TR_SLIDE) == 0)
		tr_name = "slide_transition";
	else
		tr_name = "fade_transition";

	config->randomize = obs_data_get_bool(settings, S_RANDOMIZE);
	config->loop = obs_data_get_bool(settings, S_LOOP);
	config->hide = obs_data_get_bool(settings, S_HIDE);

	new_duration = (uint32_t)obs_data_get_int(settings, S_SLIDE_TIME);
	new_speed = (uint32_t)obs_data_get_int(settings, S_TR_SPEED);

	config->mode = (strcmp(load_mode, S_LOADMODE_ONDEMAND) == 0)
			       ? SLIDESHOW_MODE_ON_DEMAND
			       : SLIDESHOW_MODE_PRELOAD;

	if (strcmp(tr_name, "cut_transition") != 0) {
		if (new_duration < 100)
			new_duration = 100;

		new_duration += new_speed;
	} else {
		if (new_duration < 50)
			new_duration = 50;
	}

	config->tr_speed = new_speed;
	config->tr_name = bstrdup(tr_name);
	config->slide_time = (float)new_duration / 1000.0f;

	const char *res_str = obs_data_get_string(settings, S_CUSTOM_SIZE);
	parse_resolution(res_str, &config->cx, &config->cy);

	files_array = obs_data_get_array(settings, S_FILES);
	da_init(config->file_paths);
	read_file_list(files_array, &config->file_paths);
	obs_data_array_release(files_array);
}

static void ss2_update(void *data, obs_data_t *settings)
{
	struct slideshow2 *ss = data;
	struct slideshow2_config new_config = {0};

	DARRAY_char_p old_files;
	DARRAY_entry cleanup;
	DARRAY_entry new_entries;
	char *old_tr_name;
	obs_source_t *new_tr = NULL;
	obs_source_t *old_tr = NULL;
	uint64_t mem_usage = 0;

	/* ------------------------------------- */
	/* get settings data */

	da_init(old_files);
	da_init(cleanup);
	da_init(new_entries);

	read_config(settings, &new_config);

	lock_mutex(ss);
	{
		/* Clear the most important lists. The slideshow is "invalid"
		 * for a short amount of time since there are no entries.
		 */
		da_move(old_files, ss->config.file_paths);
		clear_all_entries(ss, &cleanup);

		old_tr_name = bstrdup(ss->config.tr_name);
	}
	unlock_mutex(ss);

	/* Do expensive work outside of mutex to avoid blocking the render thread. */

	free_entries(&cleanup);
	update_entries(ss, new_config.mode == SLIDESHOW_MODE_PRELOAD,
		       &new_config.file_paths, &new_entries, &mem_usage);

	if (old_tr_name == NULL || strcmp(new_config.tr_name, old_tr_name) != 0)
		new_tr = obs_source_create_private(new_config.tr_name, NULL,
						   NULL);

	/* Expensive work is done, now apply the configuration. */

	lock_mutex(ss);
	{
		/* Memory management notes: `ss->config.file_paths` has already
		 * been handled by moving its content to `old_files`.
		 */

		bfree(ss->config.tr_name);
		memcpy(&ss->config, &new_config, sizeof(new_config));

		if (new_tr != NULL) {
			old_tr = ss->transition;
			ss->transition = new_tr;
		}

		da_move(ss->entries, new_entries);
		ss->mem_usage = mem_usage;

		if (ss->config.randomize) {
			set_cur_item(ss, random_index(ss, SIZE_MAX));
		} else {
			set_cur_item(ss, 0);
		}
		ss->elapsed = 0.0f;
		ss->paused = ss->config.manual;
		obs_transition_set_size(ss->transition, new_config.cx,
					new_config.cy);
		obs_transition_set_alignment(ss->transition, OBS_ALIGN_CENTER);
		obs_transition_set_scale_type(ss->transition,
					      OBS_TRANSITION_SCALE_ASPECT);
		if (new_tr != NULL)
			obs_source_add_active_child(ss->source, new_tr);
		if (ss->config.file_paths.num > 0)
			do_transition(ss, false);
	}
	unlock_mutex(ss);

	if (old_tr != NULL)
		obs_source_release(old_tr);
	free_files(&old_files);

	da_free(old_files);
	da_free(cleanup);
	da_free(new_entries);
}

/* ------------------------------------------------------------------------- */
/* Hotkeys */

static void play_pause_hotkey(void *data, obs_hotkey_id id,
			      obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);

	struct slideshow2 *ss = data;

	if (pressed && obs_source_active(ss->source)) {
		lock_mutex(ss);
		ss->paused = !ss->paused;
		unlock_mutex(ss);
	}
}

static void restart_hotkey(void *data, obs_hotkey_id id, obs_hotkey_t *hotkey,
			   bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);

	struct slideshow2 *ss = data;

	if (pressed && obs_source_active(ss->source)) {
		lock_mutex(ss);

		ss->elapsed = 0.0f;
		if (ss->config.randomize) {
			set_cur_item(ss, random_index(ss, SIZE_MAX));
		} else {
			set_cur_item(ss, 0);
		}

		obs_source_t *source =
			ss->entries.num > 0
				? obtain_cached_source(ss, ss->cur_item)
				: NULL;
		obs_transition_set(ss->transition, source);
		obs_source_release(source);

		ss->stop = false;
		ss->paused = ss->config.manual;

		unlock_mutex(ss);
	}
}

static void stop_hotkey(void *data, obs_hotkey_id id, obs_hotkey_t *hotkey,
			bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);

	struct slideshow2 *ss = data;

	if (pressed && obs_source_active(ss->source)) {
		lock_mutex(ss);

		ss->elapsed = 0.0f;
		set_cur_item(ss, 0);

		do_transition(ss, true);
		ss->stop = true;
		ss->paused = false;

		unlock_mutex(ss);
	}
}

static void next_slide_hotkey(void *data, obs_hotkey_id id,
			      obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);

	struct slideshow2 *ss = data;

	if (!ss->paused)
		return;

	if (pressed && obs_source_active(ss->source)) {
		lock_mutex(ss);

		if (ss->entries.num == 0 ||
		    obs_transition_get_time(ss->transition) < 1.0f) {
			unlock_mutex(ss);
			return;
		}

		set_next_item(ss);
		do_transition(ss, false);

		unlock_mutex(ss);
	}
}

static void previous_slide_hotkey(void *data, obs_hotkey_id id,
				  obs_hotkey_t *hotkey, bool pressed)
{
	UNUSED_PARAMETER(id);
	UNUSED_PARAMETER(hotkey);

	struct slideshow2 *ss = data;

	if (!ss->paused)
		return;

	if (pressed && obs_source_active(ss->source)) {
		lock_mutex(ss);

		if (!ss->entries.num ||
		    obs_transition_get_time(ss->transition) < 1.0f) {
			unlock_mutex(ss);
			return;
		}

		set_previous_item(ss);
		do_transition(ss, false);

		unlock_mutex(ss);
	}
}

/* ------------------------------------------------------------------------- */
/* Source implementation */

static const char *ss2_getname(void *unused)
{
	UNUSED_PARAMETER(unused);
	return obs_module_text("SlideShow2");
}

static void ss2_destroy(void *data)
{
	struct slideshow2 *ss = data;

	if (ss->cache_thread_created) {
		ss->shutdown = true;
		os_event_signal(ss->cache_event);
		pthread_join(ss->cache_thread, NULL);
	}

	free_entries(&ss->entries);

	da_free(ss->entries);
	da_free(ss->cache_queue);
	da_free(ss->cached_entries_set);

	os_event_destroy(ss->cache_event);
	obs_source_release(ss->transition);
	bfree(ss->config.tr_name);
	free_files(&ss->config.file_paths);
	pthread_mutex_destroy(&ss->mutex);

	bfree(ss);
}

static void *ss2_create(obs_data_t *settings, obs_source_t *source)
{
	struct slideshow2 *ss = bzalloc(sizeof(*ss));

	ss->source = source;

	ss->config.mode = SLIDESHOW_MODE_PRELOAD;
	ss->config.manual = false;
	ss->paused = false;
	ss->stop = false;

	ss->play_pause_hotkey = obs_hotkey_register_source(
		source, "SlideShow2.PlayPause",
		obs_module_text("SlideShow.PlayPause"), play_pause_hotkey, ss);

	ss->restart_hotkey = obs_hotkey_register_source(
		source, "SlideShow2.Restart",
		obs_module_text("SlideShow.Restart"), restart_hotkey, ss);

	ss->stop_hotkey = obs_hotkey_register_source(
		source, "SlideShow2.Stop", obs_module_text("SlideShow.Stop"),
		stop_hotkey, ss);

	ss->prev_hotkey = obs_hotkey_register_source(
		source, "SlideShow2.NextSlide",
		obs_module_text("SlideShow.NextSlide"), next_slide_hotkey, ss);

	ss->prev_hotkey = obs_hotkey_register_source(
		source, "SlideShow2.PreviousSlide",
		obs_module_text("SlideShow.PreviousSlide"),
		previous_slide_hotkey, ss);

	pthread_mutex_init_value(&ss->mutex);
	if (pthread_mutex_init(&ss->mutex, NULL) != 0)
		goto error;

	if (os_event_init(&ss->cache_event, OS_EVENT_TYPE_MANUAL) != 0)
		goto error;
	if (pthread_create(&ss->cache_thread, NULL, cache_thread, ss) != 0)
		goto error;
	ss->cache_thread_created = true;

	obs_source_update(source, NULL);

	UNUSED_PARAMETER(settings);
	return ss;

error:
	ss2_destroy(ss);
	return NULL;
}

static void ss2_video_render(void *data, gs_effect_t *effect)
{
	struct slideshow2 *ss = data;
	obs_source_t *transition = obtain_transition(ss);

	if (transition) {
		obs_source_video_render(transition);
		obs_source_release(transition);
	}

	UNUSED_PARAMETER(effect);
}

static void ss2_video_tick(void *data, float seconds)
{
	struct slideshow2 *ss = data;

	lock_mutex(ss);

	if (!ss->transition || !ss->config.slide_time)
		goto out;

	if (ss->restart_on_activate && !ss->config.randomize && ss->use_cut) {
		ss->elapsed = 0.0f;
		set_cur_item(ss, 0);
		do_transition(ss, false);
		ss->restart_on_activate = false;
		ss->use_cut = false;
		ss->stop = false;
		goto out;
	}

	if (ss->pause_on_deactivate || ss->stop || ss->paused)
		goto out;

	/* ----------------------------------------------------- */
	/* fade to transparency when the file list becomes empty */
	if (!ss->entries.num) {
		obs_source_t *active_transition_source =
			obs_transition_get_active_source(ss->transition);

		if (active_transition_source) {
			obs_source_release(active_transition_source);
			do_transition(ss, true);
		}
	}

	/* ----------------------------------------------------- */
	/* do transition when slide time reached                 */
	ss->elapsed += seconds;

	if (ss->elapsed > ss->config.slide_time) {
		ss->elapsed -= ss->config.slide_time;

		if (!ss->config.loop && ss->cur_item == ss->entries.num - 1) {
			if (ss->config.hide)
				do_transition(ss, true);
			else
				do_transition(ss, false);

			goto out;
		}

		set_next_item(ss);

		if (ss->entries.num != 0)
			do_transition(ss, false);
	} else if (ss->retry_transition) {
		debug("Retrying transition");
		do_transition(ss, false);
	}

out:
	unlock_mutex(ss);
}

static inline bool ss2_audio_render_(obs_source_t *transition, uint64_t *ts_out,
				     struct obs_source_audio_mix *audio_output,
				     uint32_t mixers, size_t channels,
				     size_t sample_rate)
{
	struct obs_source_audio_mix child_audio;
	uint64_t source_ts;

	if (obs_source_audio_pending(transition))
		return false;

	source_ts = obs_source_get_audio_timestamp(transition);
	if (!source_ts)
		return false;

	obs_source_get_audio_mix(transition, &child_audio);
	for (size_t mix = 0; mix < MAX_AUDIO_MIXES; mix++) {
		if ((mixers & (1 << mix)) == 0)
			continue;

		for (size_t ch = 0; ch < channels; ch++) {
			float *out = audio_output->output[mix].data[ch];
			float *in = child_audio.output[mix].data[ch];

			memcpy(out, in,
			       AUDIO_OUTPUT_FRAMES * MAX_AUDIO_CHANNELS *
				       sizeof(float));
		}
	}

	*ts_out = source_ts;

	UNUSED_PARAMETER(sample_rate);
	return true;
}

static bool ss2_audio_render(void *data, uint64_t *ts_out,
			     struct obs_source_audio_mix *audio_output,
			     uint32_t mixers, size_t channels,
			     size_t sample_rate)
{
	struct slideshow2 *ss = data;
	obs_source_t *transition = obtain_transition(ss);
	bool success;

	if (!transition)
		return false;

	success = ss2_audio_render_(transition, ts_out, audio_output, mixers,
				    channels, sample_rate);

	obs_source_release(transition);
	return success;
}

static void ss2_enum_sources(void *data, obs_source_enum_proc_t cb, void *param)
{
	struct slideshow2 *ss = data;

	lock_mutex(ss);
	if (ss->transition)
		cb(ss->source, ss->transition, param);
	unlock_mutex(ss);
}

static uint32_t ss2_width(void *data)
{
	struct slideshow2 *ss = data;
	lock_mutex(ss);
	uint32_t result = ss->transition ? ss->config.cx : 0;
	unlock_mutex(ss);
	return result;
}

static uint32_t ss2_height(void *data)
{
	struct slideshow2 *ss = data;
	lock_mutex(ss);
	uint32_t result = ss->transition ? ss->config.cy : 0;
	unlock_mutex(ss);
	return result;
}

static void ss2_defaults(obs_data_t *settings)
{
	obs_data_set_default_string(settings, S_LOADMODE, S_LOADMODE_PRELOAD);
	obs_data_set_default_string(settings, S_TRANSITION, "fade");
	obs_data_set_default_int(settings, S_SLIDE_TIME, 8000);
	obs_data_set_default_int(settings, S_TR_SPEED, 700);
	obs_data_set_default_string(settings, S_CUSTOM_SIZE, "1024x768");
	obs_data_set_default_string(settings, S_BEHAVIOR,
				    S_BEHAVIOR_ALWAYS_PLAY);
	obs_data_set_default_string(settings, S_SLIDEMODE, S_SLIDEMODE_AUTO);
	obs_data_set_default_bool(settings, S_LOOP, true);
}

static const char *file_filter =
	"Image files (*.bmp *.tga *.png *.jpeg *.jpg *.gif)";

static const char *aspects[] = {"16:9", "16:10", "4:3", "1:1"};

#define NUM_ASPECTS (sizeof(aspects) / sizeof(const char *))

static obs_properties_t *ss2_properties(void *data)
{
	obs_properties_t *ppts = obs_properties_create();
	struct slideshow2 *ss = data;
	struct obs_video_info ovi;
	struct dstr path = {0};
	obs_property_t *p;
	int cx;
	int cy;

	/* ----------------- */

	obs_get_video_info(&ovi);
	cx = (int)ovi.base_width;
	cy = (int)ovi.base_height;

	/* ----------------- */

	p = obs_properties_add_list(ppts, S_LOADMODE, T_LOADMODE,
				    OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_STRING);
	obs_property_list_add_string(p, T_LOADMODE_PRELOAD, S_LOADMODE_PRELOAD);
	obs_property_list_add_string(p, T_LOADMODE_ONDEMAND,
				     S_LOADMODE_ONDEMAND);

	p = obs_properties_add_list(ppts, S_BEHAVIOR, T_BEHAVIOR,
				    OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_STRING);
	obs_property_list_add_string(p, T_BEHAVIOR_ALWAYS_PLAY,
				     S_BEHAVIOR_ALWAYS_PLAY);
	obs_property_list_add_string(p, T_BEHAVIOR_STOP_RESTART,
				     S_BEHAVIOR_STOP_RESTART);
	obs_property_list_add_string(p, T_BEHAVIOR_PAUSE_UNPAUSE,
				     S_BEHAVIOR_PAUSE_UNPAUSE);

	p = obs_properties_add_list(ppts, S_SLIDEMODE, T_MODE,
				    OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_STRING);
	obs_property_list_add_string(p, T_MODE_AUTO, S_SLIDEMODE_AUTO);
	obs_property_list_add_string(p, T_MODE_MANUAL, S_SLIDEMODE_MANUAL);

	p = obs_properties_add_list(ppts, S_TRANSITION, T_TRANSITION,
				    OBS_COMBO_TYPE_LIST,
				    OBS_COMBO_FORMAT_STRING);
	obs_property_list_add_string(p, T_TR_CUT, TR_CUT);
	obs_property_list_add_string(p, T_TR_FADE, TR_FADE);
	obs_property_list_add_string(p, T_TR_SWIPE, TR_SWIPE);
	obs_property_list_add_string(p, T_TR_SLIDE, TR_SLIDE);

	obs_properties_add_int(ppts, S_SLIDE_TIME, T_SLIDE_TIME, 50, 3600000,
			       50);
	obs_properties_add_int(ppts, S_TR_SPEED, T_TR_SPEED, 0, 3600000, 50);
	obs_properties_add_bool(ppts, S_LOOP, T_LOOP);
	obs_properties_add_bool(ppts, S_HIDE, T_HIDE);
	obs_properties_add_bool(ppts, S_RANDOMIZE, T_RANDOMIZE);

	p = obs_properties_add_list(ppts, S_CUSTOM_SIZE, T_CUSTOM_SIZE,
				    OBS_COMBO_TYPE_EDITABLE,
				    OBS_COMBO_FORMAT_STRING);

	for (size_t i = 0; i < NUM_ASPECTS; i++)
		obs_property_list_add_string(p, aspects[i], aspects[i]);

	char str[32];
	snprintf(str, 32, "%dx%d", cx, cy);
	obs_property_list_add_string(p, str, str);

	if (ss) {
		lock_mutex(ss);
		if (ss->config.file_paths.num > 0) {
			char **last = da_end(ss->config.file_paths);
			const char *slash;

			dstr_copy(&path, *last);
			dstr_replace(&path, "\\", "/");
			slash = strrchr(path.array, '/');
			if (slash)
				dstr_resize(&path, slash - path.array + 1);
		}
		unlock_mutex(ss);
	}

	obs_properties_add_editable_list(ppts, S_FILES, T_FILES,
					 OBS_EDITABLE_LIST_TYPE_FILES,
					 file_filter, path.array);
	dstr_free(&path);

	return ppts;
}

static void ss2_activate(void *data)
{
	struct slideshow2 *ss = data;

	if (ss->config.behavior == BEHAVIOR_STOP_RESTART) {
		ss->restart_on_activate = true;
		ss->use_cut = true;
	} else if (ss->config.behavior == BEHAVIOR_PAUSE_UNPAUSE) {
		ss->pause_on_deactivate = false;
	}
}

static void ss2_deactivate(void *data)
{
	struct slideshow2 *ss = data;

	if (ss->config.behavior == BEHAVIOR_PAUSE_UNPAUSE)
		ss->pause_on_deactivate = true;
}

struct obs_source_info slideshow2_info = {
	.id = "slideshow2",
	.type = OBS_SOURCE_TYPE_INPUT,
	.output_flags = OBS_SOURCE_VIDEO | OBS_SOURCE_CUSTOM_DRAW |
			OBS_SOURCE_COMPOSITE,
	.get_name = ss2_getname,
	.create = ss2_create,
	.destroy = ss2_destroy,
	.update = ss2_update,
	.activate = ss2_activate,
	.deactivate = ss2_deactivate,
	.video_render = ss2_video_render,
	.video_tick = ss2_video_tick,
	.audio_render = ss2_audio_render,
	.enum_active_sources = ss2_enum_sources,
	.get_width = ss2_width,
	.get_height = ss2_height,
	.get_defaults = ss2_defaults,
	.get_properties = ss2_properties,
	.icon_type = OBS_ICON_TYPE_SLIDESHOW,
};
