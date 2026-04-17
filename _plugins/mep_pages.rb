# _plugins/mep_pages.rb
#
# Generates one HTML page per MEP from the individual JSON files
# written to _data/meps/<id>.json by the Python fetch script.
#
# Each page is rendered at /meps/<id>/index.html using _layouts/mep.html.
# The MEP's data is available in the layout via `page.mep_id` and
# site.data.meps[page.mep_id] (Jekyll auto-loads all _data/ files).

module EPTracker
  class MepPage < Jekyll::Page
    def initialize(site, base, mep_id, mep_data)
      @site = site
      @base = base
      @dir  = File.join("meps", mep_id.to_s)
      @name = "index.html"

      process(@name)

      @data = {
        "layout"   => "mep",
        "title"    => mep_data["full_name"] || mep_id,
        "mep_id"   => mep_id,
        "permalink" => "/meps/#{mep_id}/",
        # SEO helpers
        "description" => [
          mep_data["full_name"],
          mep_data["group_name"],
          mep_data["country"],
        ].compact.join(" · "),
      }
    end

    def url_placeholders
      { path: @dir, output_ext: "" }
    end
  end

  class MepPageGenerator < Jekyll::Generator
    safe true
    priority :normal

    def generate(site)
      # Static stubs in _pages/meps/ are the primary mechanism now.
      # Only run this generator as a fallback if no stubs exist.
      stubs_dir = File.join(site.source, "_pages", "meps")
      if Dir.exist?(stubs_dir) && !Dir.glob(File.join(stubs_dir, "*.md")).empty?
        Jekyll.logger.info "EP Tracker:", "Skipping plugin — static MEP stubs found in _pages/meps/"
        return
      end

      meps_data_dir = File.join(site.source, "_data", "meps")
      return unless Dir.exist?(meps_data_dir)

      mep_files = Dir.glob(File.join(meps_data_dir, "*.json"))
      Jekyll.logger.info "EP Tracker:", "Generating #{mep_files.size} MEP pages (no stubs found)…"

      mep_files.each do |file|
        mep_id   = File.basename(file, ".json")
        mep_data = begin
          JSON.parse(File.read(file))
        rescue JSON::ParserError => e
          Jekyll.logger.warn "EP Tracker:", "Could not parse #{file}: #{e.message}"
          next
        end

        site.pages << MepPage.new(site, site.source, mep_id, mep_data)
      end
    end
  end
end
