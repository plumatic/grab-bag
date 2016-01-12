(require 'package)
(add-to-list 'package-archives
             '("marmalade" . "http://marmalade-repo.org/packages/"))
;;(add-to-list 'package-archives
;;             '("melpa" . "http://melpa.milkbox.net/packages/") t)
(package-initialize)
(defun install-if (name)
  (when (not (package-installed-p name))
    (package-install name)))

(when (not (package-installed-p 'clojure-mode))
  (package-refresh-contents))

(install-if 'cljsbuild-mode)
(install-if 'clojurescript-mode)
(install-if 'clojure-mode)
(install-if 'magit)
(install-if 'cider)
(install-if 'clojure-test-mode)
(install-if 'paredit)
(install-if 'autopair)
(install-if 'auto-complete)
(install-if 'textmate)
(install-if 'clj-refactor)

(require 'cljsbuild-mode)
(require 'autopair)
(require 'auto-complete-config)
(require 'cider)
(require 'clj-refactor)

(ac-config-default)
(setq ac-delay 0.2)

(show-paren-mode 1)

;; enable auto revert. It will refresh the buffers when a file changes and the buffer isn't dirty
(global-auto-revert-mode 1)

;; disable bell function
(setq ring-bell-function 'ignore)

;; disable toolbar
(tool-bar-mode -1)

;; disable splash screen
(custom-set-variables '(inhibit-startup-screen t))

(require 'uniquify)
(setq uniquify-buffer-name-style 'forward)

;;get rid of annoying differening versions message

(global-set-key (kbd "s-=") `hs-show-block)
(global-set-key (kbd "s--") `hs-hide-block)
(global-set-key (kbd "s-+") `hs-show-all)
(global-set-key (kbd "s-_") `hs-hide-all)
(global-set-key (kbd "C-M-,") `tags-loop-continue)

;; These clobber normal char fns, can still access them at CM-
;;(global-set-key (kbd "C-f") `paredit-forward)
;;(global-set-key (kbd "C-b") `paredit-backward)
(global-set-key (kbd "S-s-<left>") 'paredit-forward-barf-sexp )
(global-set-key (kbd "S-s-<right>") 'paredit-forward-slurp-sexp)

(setq ido-enable-flex-matching t)
(setq ido-everywhere t)
(ido-mode 1)

(defun clojure-mode-custom-indent ()
  (put-clojure-indent 'fnk 'defun)
  (put-clojure-indent 'defnk 'defun)
  (put-clojure-indent 'for-map 1)
  (put-clojure-indent 'instance 2)
  (put-clojure-indent 'inline 1)
  (put-clojure-indent 'letk 1)
  (put-clojure-indent 'when-letk 1)
  (put-clojure-indent 'go-loop 1)
  (put-clojure-indent 'this-as 'defun)
  (put-clojure-indent 'when-some '1)
  (put-clojure-indent 'if-some '1)
  (put 'specify 'clojure-backtracking-indent '((2)))
  (put 'specify! 'clojure-backtracking-indent '((2)))
  (put 'defcomponent 'clojure-backtracking-indent '((2)))
  (put 'defcomponentk 'clojure-backtracking-indent '((2)))
  (put 'defmixin 'clojure-backtracking-indent '((2)))
  (put 'clojure.core/defrecord 'clojure-backtracking-indent '(4 4 (2)))
  (put 's/defrecord 'clojure-backtracking-indent '(4 4 (2)))
  (put 's/defrecord+ 'clojure-backtracking-indent '(4 4 (2)))
  (put 'potemkin/deftype+ 'clojure-backtracking-indent '(4 4 (2)))
  (put 'potemkin/defrecord+ 'clojure-backtracking-indent '(4 4 (2))))


;;; all code in this function lifted from the clojure-mode function
;;; from clojure-mode.el

;; JW: This breaks my nrepl syntax highlighting.
;; (defun clojure-font-lock-setup ()
;;   (interactive)
;;   (set (make-local-variable 'lisp-indent-function)
;;        'clojure-indent-function)
;;   (set (make-local-variable 'lisp-doc-string-elt-property)
;;        'clojure-doc-string-elt)
;;   (set (make-local-variable 'font-lock-multiline) t)

;;   (add-to-list 'font-lock-extend-region-functions
;;                'clojure-font-lock-extend-region-def t)

;;   (when clojure-mode-font-lock-comment-sexp
;;     (add-to-list 'font-lock-extend-region-functions
;;                  'clojure-font-lock-extend-region-comment t)
;;     (make-local-variable 'clojure-font-lock-keywords)
;;     (add-to-list 'clojure-font-lock-keywords
;;                  'clojure-font-lock-mark-comment t)
;;     (set (make-local-variable 'open-paren-in-column-0-is-defun-start) nil))

;;   (setq font-lock-defaults
;;         '(clojure-font-lock-keywords    ; keywords
;;           nil nil
;;           (("+-*/.<>=!?$%_&~^:@" . "w")) ; syntax alist
;;           nil
;;           (font-lock-mark-block-function . mark-defun)
;;           (font-lock-syntactic-face-function
;;            . lisp-font-lock-syntactic-face-function))))


(defun indent-or-expand (arg)
  "Either indent according to mode, or expand the word preceding
point."
  (interactive "*P")
  (if (and
       (or (bobp) (= ?w (char-syntax (char-before))))
       (or (eobp) (not (= ?w (char-syntax (char-after))))))
      (dabbrev-expand arg)
    (indent-according-to-mode)))

(defun my-tab-fix ()
  (local-set-key [tab] 'indent-or-expand))

;;; Support fixing ns declaration!

(defun nrepl-interactive-eval-print-ns-handler (buffer region)
  "Make a handler for evaluating and printing result in BUFFER."
  (nrepl-make-response-handler
   buffer
   (lexical-let ((region region))
     (lambda (buffer value)
       (with-current-buffer buffer
         (apply #'delete-region region)
         (insert (format "%s" (car (read-from-string value))))
         (when (clojure-find-ns)
           (save-excursion
             (goto-char (match-beginning 0))))
         (indent-whole-buffer))))
   '()
   (lambda (buffer err)
     (message "%s" err))
   '()))

(defun nrepl-interactive-eval-print-ns (form region)
  "Evaluate the given FORM and print value in current buffer."
  (let ((buffer (current-buffer)))
    (nrepl-send-string form
                       (nrepl-interactive-eval-print-ns-handler buffer region)
                       (cider-current-ns))))

(defun indent-whole-buffer ()
  "indent whole buffer"
  (interactive)
  (delete-trailing-whitespace)
  (indent-region (point-min) (point-max) nil)
  (untabify (point-min) (point-max)))

(defun nrepl-fix-ns-decl ()
  (interactive)
  (cider-load-current-buffer)
  (when (clojure-find-ns)
    (goto-char (match-beginning 0))
    (let ((region (cider--region-for-defun-at-point)))
      (cider-interactive-eval "(require 'plumbing.organize-ns)")
      (nrepl-interactive-eval-print-ns
       (format "(plumbing.organize-ns/nrepl-organized-ns '%s)"
               (cider-sexp-at-point))
       region))))

(defun nrepl-eval-expression-at-point-in-ns ()
  "Evaluate the current toplevel form in the current buffer's namespace."
  (interactive)
  (let ((ns-form (when (clojure-find-ns)
                   (save-excursion
                     (goto-char (match-beginning 0))
                     (cider-sexp-at-point))))
        (cur-form (cider-sexp-at-point)))
    (cider-interactive-eval (format "%s %s" ns-form cur-form))))


;; TODO: don't clear buffer, use more global variables FTW!
(defvar i-suck)
(defun nrepl-clear-repl-and-test ()
  (interactive)
  (setq i-suck (current-buffer))
  (cider-find-and-clear-repl-buffer)
  (clojure-test-run-tests))


(require 'whitespace)
(setq-default whitespace-style
              '(face tabs lines-tail indentation::space))
(setq-default whitespace-line-column 95)

(add-hook 'textmate-mode-hook
  (lambda ()
    (define-key *textmate-mode-map* (kbd "M-<up>") nil)))

(add-hook 'clojure-mode-hook
          #'(lambda ()
              (textmate-mode)
              (paredit-mode 1)
              (autopair-mode)
              (hs-minor-mode 1)
              (clojure-mode-custom-indent)
              (my-tab-fix)
              (show-paren-mode 1)
              (cider-mode)
              (whitespace-mode)
              (local-set-key (kbd "C-c C-c") 'nrepl-eval-expression-at-point-in-ns)
              (local-set-key (kbd "M-<up>") 'paredit-splice-sexp-killing-backward)
              (local-set-key (kbd "C-c C-.") 'nrepl-fix-ns-decl)
              (local-set-key (kbd "C-c C-i") 'indent-whole-buffer)
              (local-set-key (kbd "C-c C-/") 'nrepl-clear-repl-and-test)
              (add-hook 'before-save-hook 'indent-whole-buffer nil t)
              (clj-refactor-mode 1)
              (cljr-add-keybindings-with-prefix "C-c C-a")
              ))

(add-hook 'clojure-test-mode-hook
          #'(lambda ()
              ;; sorry.  
              (defun clojure-test-extract-results (buffer results)
                (with-current-buffer buffer
                  (let ((result-vars (read results)))
                    (mapc #'clojure-test-extract-result result-vars)
                    (clojure-test-echo-results))
                  (cider-switch-to-repl-buffer)
                  (goto-line 0))
                (pop-to-buffer i-suck))))

;;no tabs!!!
(setq c-basic-offset 2)
(setq tab-width 2)
(setq indent-tabs-mode nil)

;; current buffer name in title bar
(setq frame-title-format "%b")

;; start emacs server
(server-start)

(defun move-line-down ()
  (interactive)
  (let ((col (current-column)))
    (save-excursion
      (next-line)
      (transpose-lines 1))
    (next-line)
    (move-to-column col)))

(defun move-line-up ()
  (interactive)
  (let ((col (current-column)))
    (save-excursion
      (next-line)
      (transpose-lines -1))
    (move-to-column col)))

(global-set-key (kbd "M-<down>") 'move-line-down)
(global-set-key (kbd "M-<up>") 'move-line-up)
(global-set-key (kbd "M-S-<down>") 'duplicate-line)

(global-set-key [(f2)] 'slime-eval-defun)
(global-set-key [(f3)] 'slime-eval-last-expression)


(defun duplicate-line()
  (interactive)
  (move-beginning-of-line 1)
  (kill-line)
  (yank)
  (newline)
  (yank)
  )

(setq x-select-enable-clipboard t)

;;enable deleting selected text with del or ctrl-d
(delete-selection-mode t)

(add-to-list 'auto-mode-alist '("\\.clj$" . clojure-mode))
(add-to-list 'auto-mode-alist '("\\.cljx$" . clojure-mode))
;;(add-to-list 'auto-mode-alist '("\\.clj$" . cider-mode))
(global-set-key "\C-x\C-m" 'execute-extended-command)

(prefer-coding-system 'utf-8)
(set-language-environment "UTF-8")

;; No vertial splits
(setq split-height-threshold nil)

;; Every horizontal split
(setq split-width-threshold 200)



;; nREPL
(setq nrepl-history-file "~/.nrepl-history")

(defun fix-paredit-repl ()
  (interactive)
  (local-set-key "{" 'paredit-open-curly)
  (local-set-key "}" 'paredit-close-curly)
  (local-set-key (kbd "M-<up>") 'paredit-splice-sexp-killing-backward)
  (modify-syntax-entry ?\{ "(}")
  (modify-syntax-entry ?\} "){")
  (modify-syntax-entry ?\[ "(]")
  (modify-syntax-entry ?\] ")["))

(add-hook 'cider-mode-hook 'cider-turn-on-eldoc-mode)

(add-hook 'nrepl-interaction-mode-hook
          #'(lambda () 
	      (cider-turn-on-eldoc-mode)
	      (define-key cider-mode-map (kbd "C-c C-c") 'nrepl-eval-expression-at-point-in-ns)))

(setq nrepl-tab-command 'indent-for-tab-command)

;; This means you don't get stack traces from the repl -- put in your .emacs if you want it.
;; (setq nrepl-popup-stacktraces nil)
(setq cider-repl-popup-stacktraces t)
(setq nrepl-buffer-name-show-port t)
;; (cider-repl-toggle-pretty-printing) ;; TODO test if works after cider upgrade (previously fucked with def)

(defun ansi-color-buffer (ignore1 ignore2)
  (interactive "r")
  (ansi-color-apply-on-region (point-min) (point-max)))

(add-hook 'cider-repl-mode-hook
          #'(lambda ()
              (paredit-mode 1)
              (local-set-key (kbd "C-c C-c") 'cider-interrupt)
              (local-set-key (kbd "C-c C-a") 'ansi-color-buffer)
              (fix-paredit-repl)
              (show-paren-mode 1)
              (subword-mode 1)
              ;; (font-lock-mode nil)
              ;; (clojure-font-lock-setup)
              ;; (font-lock-mode t)
              ))

(defun lein-nrepl ()
  "Jenny didn't like typing nrepl-jack-in"
  (interactive)
  (if (get-buffer (nrepl-server-buffer-name))
      (cider-restart)
    (cider-jack-in)))



;; This blocks my color theme from working.
;; I moved it into the install script.
;; (load-theme 'tango-dark)


;; More tweaks from jason.

(defun back-window ()
  (interactive)
  (other-window -1))

(global-set-key (kbd "C-x p") `back-window)

;; Shut up "fontifying" thing
(setq font-lock-verbose nil)

(setq clojure-mode-use-backtracking-indent t)

(setq backup-directory-alist
      '(("." . "~/.emacs_backups")))


 ;;; Change behavior of enter key for paredit mode

(setq skeleton-pair t)
(setq skeleton-pair-alist
      '((?\( _ ?\))
        (?[  _ ?])
        (?{  _ ?})
        (?\" _ ?\")))
(defun autopairs-ret (arg)
  (interactive "P")
  (let (pair)
    (dolist (pair skeleton-pair-alist)
      (when (eq (char-after) (car (last pair)))
        (save-excursion (newline-and-indent))))
    (newline arg)
    (indent-according-to-mode)))

(global-set-key (kbd "RET") 'autopairs-ret)

;; whitespace
(setq-default indent-tabs-mode nil)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Support for schema/defn in eldoc

(defun nrepl-eldoc ()
  "Backend function for eldoc to show argument list in the echo area."
  (when (nrepl-current-connection-buffer)
    (let* ((info (cider-eldoc-info-in-current-sexp))
           (thing (car info))
           (pos (cadr info))
           (form (format "(try
                  (let [m (-> \"%s\"
                            clojure.core/read-string
                            clojure.core/resolve
                            clojure.core/meta)]
                     (or (:raw-arglists m)
                         (:arglists m)))
                   (catch Throwable t nil))" thing))
           (result (when thing
                     (nrepl-send-string-sync form
                                             nrepl-buffer-ns
                                             (nrepl-current-tooling-session))))
           (value (plist-get result :value)))
      (unless (string= value "nil")
        (format "%s: %s"
                (cider-eldoc-format-thing thing)
                (cider-eldoc-format-arglist value pos))))))

(setq cider-auto-select-error-buffer t)
